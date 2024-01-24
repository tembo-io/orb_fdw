use pgrx::warning;
use pgrx::{pg_sys, prelude::*, JsonB};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use supabase_wrappers::prelude::*;
use tokio::runtime::Runtime;
pgrx::pg_module_magic!();
use futures::StreamExt;
use orb_billing::{Client, ClientConfig, ListParams, SubscriptionListParams};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[derive(Error, Debug)]
enum OrbFdwError {
    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("parse JSON response failed: {0}")]
    JsonParseError(#[from] serde_json::Error),
}

impl From<OrbFdwError> for ErrorReport {
    fn from(value: OrbFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type OrbFdwResult<T> = Result<T, OrbFdwError>;

fn body_to_rows(
    resp: &JsonValue,
    obj_key: &str,
    normal_cols: Vec<(&str, &str, &str)>,
    tgt_cols: &[Column],
) -> Vec<Row> {
    let mut result = Vec::new();

    let objs = if resp.is_array() {
        // If `resp` is directly an array
        resp.as_array().unwrap()
    } else {
        // If `resp` is an object containing the array under `obj_key`
        match resp
            .as_object()
            .and_then(|v| v.get(obj_key))
            .and_then(|v| v.as_array())
        {
            Some(objs) => objs,
            None => return result,
        }
    };

    for obj in objs {
        let mut row = Row::new();

        // extract normal columns
        for tgt_col in tgt_cols {
            if let Some((src_name, col_name, col_type)) =
                normal_cols.iter().find(|(_, c, _)| c == &tgt_col.name)
            {
                // Navigate through nested properties
                let mut current_value: Option<&JsonValue> = Some(obj);
                for part in src_name.split('.') {
                    current_value = current_value.unwrap().as_object().unwrap().get(part);
                }

                let cell = current_value.and_then(|v| match *col_type {
                    "bool" => v.as_bool().map(Cell::Bool),
                    "i64" => v.as_i64().map(Cell::I64),
                    "string" => v.as_str().map(|a| Cell::String(a.to_owned())),
                    "timestamp" => v.as_str().map(|a| {
                        let secs = a.parse::<i64>().unwrap() / 1000;
                        let ts = to_timestamp(secs as f64);
                        Cell::Timestamp(ts.to_utc())
                    }),
                    "timestamp_iso" => v.as_str().map(|a| {
                        let ts = Timestamp::from_str(a).unwrap();
                        Cell::Timestamp(ts)
                    }),
                    "json" => Some(Cell::Json(JsonB(v.clone()))),
                    _ => None,
                });
                row.push(col_name, cell);
            }
        }

        // put all properties into 'attrs' JSON column
        if tgt_cols.iter().any(|c| &c.name == "attrs") {
            let attrs = serde_json::from_str(&obj.to_string()).unwrap();
            row.push("attrs", Some(Cell::Json(JsonB(attrs))));
        }

        result.push(row);
    }
    result
}

// convert response body text to rows
fn resp_to_rows(obj: &str, resp: &JsonValue, tgt_cols: &[Column]) -> Vec<Row> {
    let mut result = Vec::new();

    match obj {
        "customers" => {
            result = body_to_rows(
                resp,
                "data",
                vec![
                    ("id", "user_id", "string"),
                    ("external_id", "organization_id", "string"),
                    ("name", "first_name", "string"),
                    ("email", "email", "string"),
                    ("payment_provider_id", "stripe_id", "string"),
                    ("created_at", "created_at", "i64"),
                ],
                tgt_cols,
            );
        }
        "subscriptions" => {
            result = body_to_rows(
                resp,
                "data",
                vec![
                    ("subscription_id", "subscription_id", "string"),
                    ("status", "status", "string"),
                    ("plan", "plan", "string"),
                    ("started_date", "started_date", "i64"),
                    ("end_date", "end_date", "i64"),
                ],
                tgt_cols,
            );
        }
        _ => {
            warning!("unsupported object: {}", obj);
        }
    }

    result
}

fn convert_to_json<T, E>(items: Vec<Result<T, E>>) -> JsonValue
where
    T: Serialize,
{
    let items: Vec<Option<T>> = items.into_iter().map(|result| result.ok()).collect();

    serde_json::to_value(items).unwrap_or_else(|_| JsonValue::Null)
}

pub(crate) struct OrbFdw {
    rt: Runtime,
    client: Option<Client>,
    scan_result: Option<Vec<Row>>,
    tgt_cols: Vec<Column>,
    iter_idx: usize,
}

impl ForeignDataWrapper<OrbFdwError> for OrbFdw {
    fn new(options: &HashMap<String, String>) -> OrbFdwResult<Self> {
        let token = if let Some(access_token) = options.get("api_key") {
            access_token.to_owned()
        } else {
            warning!("Cannot find api_key in options");
            let access_token = env::var("ORB_API_KEY").unwrap();
            access_token
        };

        let client_config = ClientConfig {
            api_key: token.to_string(),
        };
        let orb_client = Client::new(client_config);

        Ok(OrbFdw {
            rt: create_async_runtime()?,
            client: Some(orb_client),
            tgt_cols: Vec::new(),
            scan_result: None,
            iter_idx: 0,
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> OrbFdwResult<()> {
        let obj = require_option("object", options)?;

        self.scan_result = None;
        self.tgt_cols = columns.to_vec();

        if let Some(client) = &self.client {
            let mut result = Vec::new();

            self.rt.block_on(async {
                if obj == "customers" {
                    let customer_stream =
                        client.list_customers(&ListParams::DEFAULT.page_size(400));

                    let customers = customer_stream.collect::<Vec<_>>().await;
                    let orb_customer = convert_to_json(customers);
                    let mut rows = resp_to_rows(&obj, &orb_customer, &self.tgt_cols[..]);
                    result.append(&mut rows);
                } else if obj == "subscriptions" {
                    let subscriptions_stream =
                        client.list_subscriptions(&SubscriptionListParams::DEFAULT.page_size(400));

                    let subscriptions = subscriptions_stream.collect::<Vec<_>>().await;
                    let orb_subscription = convert_to_json(subscriptions);
                    let mut rows = resp_to_rows(&obj, &orb_subscription, &self.tgt_cols[..]);
                    result.append(&mut rows);
                } else {
                    warning!("unsupported object: {:#?}", obj);
                    return;
                }
            });

            self.scan_result = Some(result);
        }
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> OrbFdwResult<Option<()>> {
        if let Some(ref mut result) = self.scan_result {
            if self.iter_idx < result.len() {
                row.replace_with(result[self.iter_idx].clone());
                self.iter_idx += 1;
                return Ok(Some(()));
            }
        }
        Ok(None)
    }

    fn end_scan(&mut self) -> OrbFdwResult<()> {
        self.scan_result.take();
        Ok(())
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) -> OrbFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "object")?;
            }
        }

        Ok(())
    }
}
