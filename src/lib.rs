use pgrx::warning;
use pgrx::{pg_sys, prelude::*, JsonB};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use supabase_wrappers::prelude::*;
use tokio::runtime::Runtime;
pg_module_magic!();
mod orb_fdw;
use crate::orb_fdw::OrbFdwError;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};

fn resp_to_rows(obj: &str, resp: &JsonValue, tgt_cols: &[Column]) -> Vec<Row> {
    let mut result = Vec::new();

    match obj {
        "customers" => {
            result = body_to_rows(
                resp,
                "data",
                vec![
                    ("id", "user_id", "string"),
                    ("external_customer_id", "organization_id", "string"),
                    ("name", "first_name", "string"),
                    ("email", "email", "string"),
                    ("payment_provider_id", "stripe_id", "string"),
                    ("created_at", "created_at", "string"),
                ],
                tgt_cols,
            );
        }
        "subscriptions" => {
            result = body_to_rows(
                resp,
                "data",
                vec![
                    ("id", "subscription_id", "string"),
                    ("customer.external_customer_id", "organization_id", "string"),
                    ("status", "status", "string"),
                    ("plan.external_plan_id", "plan", "string"),
                    (
                        "current_billing_period_start_date",
                        "started_date",
                        "string",
                    ),
                    ("current_billing_period_end_date", "end_date", "string"),
                ],
                tgt_cols,
            );
        }
        "invoices" => {
            result = body_to_rows(
                resp,
                "data",
                vec![
                    ("subscription.id", "subscription_id", "string"),
                    ("customer.id", "customer_id", "string"),
                    ("customer.external_customer_id", "organization_id", "string"),
                    ("status", "status", "string"),
                    ("due_date", "due_date", "string"),
                    ("amount_due", "amount", "string"),
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

                if *src_name == "email_addresses" {
                    current_value = current_value
                        .and_then(|v| v.as_array().and_then(|arr| arr.get(0)))
                        .and_then(|first_obj| {
                            first_obj
                                .as_object()
                                .and_then(|obj| obj.get("email_address"))
                        });
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

#[wrappers_fdw(
    version = "0.0.4",
    author = "Jay Kothari",
    website = "https://github.com/orb_fdw",
    error_type = "OrbFdwError"
)]
pub(crate) struct OrbFdw {
    rt: Runtime,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
    tgt_cols: Vec<Column>,
}

impl OrbFdw {
    const FDW_NAME: &'static str = "OrbFdw";
    // convert response body text to rows

    const DEFAULT_BASE_URL: &'static str = "https://api.withorb.com/v1";
    const DEFAULT_ROWS_LIMIT: usize = 10_000;

    // TODO: will have to incorporate offset at some point
    const PAGE_SIZE: usize = 500;

    fn build_url(&self, obj: &str, cursor: Option<String>) -> String {
        let base_url = Self::DEFAULT_BASE_URL.to_owned();
        let cursor_param = if let Some(ref cur) = cursor {
            format!("&cursor={}", cur)
        } else {
            String::new()
        };

        match obj {
            "customers" => format!(
                "{}/customers?limit={}{}",
                base_url,
                Self::PAGE_SIZE,
                cursor_param
            ),
            "subscriptions" => format!(
                "{}/subscriptions?limit={}{}",
                base_url,
                Self::PAGE_SIZE,
                cursor_param
            ),
            "invoices" => format!(
                "{}/invoices?limit={}{}",
                base_url,
                Self::PAGE_SIZE,
                cursor_param
            ),
            _ => {
                warning!("unsupported object: {:#?}", obj);
                "".to_string()
            }
        }
    }
}

impl ForeignDataWrapper for OrbFdw {
    fn new(options: &HashMap<String, String>) -> Self {
        let token = if let Some(access_token) = options.get("api_key") {
            access_token.to_owned()
        } else {
            warning!("Cannot find api_key in options");
            let access_token = env::var("ORB_API_KEY").unwrap();
            access_token
        };

        let mut headers = header::HeaderMap::new();
        let value = format!("Bearer {}", token);
        let mut auth_value = header::HeaderValue::from_str(&value)
            .map_err(|_| OrbFdwError::InvalidApiKeyHeader)
            .unwrap();
        auth_value.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth_value);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let ret = Self {
            rt: create_async_runtime(),
            client: Some(client),
            tgt_cols: Vec::new(),
            scan_result: None,
        };

        ret
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) {
        let obj = match require_option("object", options) {
            Some(obj) => obj,
            None => return,
        };

        self.scan_result = None;

        if let Some(client) = &self.client {
            let mut result = Vec::new();
            let mut cursor: Option<String> = None;

            loop {
                let url = self.build_url(&obj, cursor.clone()); // Ensure build_url handles None as initial cursor
                let body = self
                    .rt
                    .block_on(client.get(&url).send())
                    .and_then(|resp| {
                        resp.error_for_status()
                            .and_then(|resp| self.rt.block_on(resp.text()))
                            .map_err(reqwest_middleware::Error::from)
                    })
                    .unwrap();

                let json: serde_json::Value = serde_json::from_str(&body).unwrap();
                let rows = resp_to_rows(&obj, &json, columns); // Assuming this function exists and works as intended
                if rows.is_empty() {
                    break;
                }
                result.append(&mut rows.clone());
                cursor = json
                    .get("pagination_metadata")
                    .and_then(|pm| pm.get("next_cursor"))
                    .and_then(|nc| nc.as_str())
                    .map(String::from);
                // Break if there is no next cursor
                if cursor.is_none() {
                    break;
                }
            }
            self.scan_result = Some(result);
        }
    }

    fn iter_scan(&mut self, row: &mut Row) -> Option<()> {
        if let Some(ref mut result) = self.scan_result {
            if !result.is_empty() {
                return result
                    .drain(0..1)
                    .last()
                    .map(|src_row| row.replace_with(src_row));
            }
        }
        None
    }

    fn end_scan(&mut self) {
        self.scan_result.take();
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "object");
            }
        }
    }
}
