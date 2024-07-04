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
use futures::StreamExt;
use orb_billing::{
    Client as OrbClient, ClientConfig as OrbClientConfig, Customer as OrbCustomer, Error,
    ListParams, Subscription as OrbSubscription, SubscriptionListParams,
};
use reqwest::{self};

// TODO: Remove all unwraps. Handle the errors
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
                        .and_then(|v| v.as_array().and_then(|arr| arr.first()))
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
    version = "0.12.0",
    author = "Jay Kothari",
    website = "https://github.com/orb_fdw",
    error_type = "OrbFdwError"
)]
pub(crate) struct OrbFdw {
    rt: Runtime,
    client: OrbClient,
    scan_result: Option<Vec<Row>>,
    tgt_cols: Vec<Column>,
}

impl OrbFdw {
    // convert response body text to rows
    const DEFAULT_BASE_URL: &'static str = "https://api.withorb.com/v1";

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

type OrbFdwResult<T> = Result<T, OrbFdwError>;
impl ForeignDataWrapper<OrbFdwError> for OrbFdw {
    fn new(options: &HashMap<String, String>) -> OrbFdwResult<Self> {
        let token = if let Some(access_token) = options.get("api_key") {
            access_token.to_owned()
        } else {
            warning!("Cannot find api_key in options");
            env::var("ORB_API_KEY").unwrap()
        };
        let client_config = OrbClientConfig {
            api_key: token.to_string(),
        };
        let orb_client = OrbClient::new(client_config);
        let rt = create_async_runtime().expect("failed to create async runtime");
        Ok(Self {
            rt,
            client: orb_client,
            scan_result: None,
            tgt_cols: Vec::new(),
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
        let obj = require_option("object", options).expect("invalid option");
        self.scan_result = None;
        let mut result = Vec::new();

        let run = self.rt.block_on(async {
            loop {
                let obj_js = match obj {
                    "customers" => {
                        let customers_stream = self
                            .client
                            .list_customers(&ListParams::DEFAULT.page_size(400));
                        let customers = customers_stream.collect::<Vec<_>>().await;

                        // // Handle potential errors in the stream
                        // let mut customers_vec = Vec::new();
                        // for customer_result in customers {
                        //     match customer_result {
                        //         Ok(customers) => info!("it worked"),
                        //         Err(e) => {
                        //             error!("Error getting all orb customers: {}", e);
                        //         }
                        //     }
                        // }
                        // info!("Found {} customers in Orb", customers_vec.len());
                        match customers {
                            Ok(customers) => {
                                serde_json::to_value(customers).expect("failed deserializing users")
                            }
                            Err(e) => {
                                warning!("Failed to get users: {}", e);
                                break;
                            }
                        }
                    }
                    _ => {
                        warning!("unsupported object: {}", obj);
                        break;
                    }
                };

                let mut rows = resp_to_rows(obj, &obj_js, &self.tgt_cols[..]);
                result.append(&mut rows);
            }
            Ok(())
        });
        run.expect("failed to run async block");
        self.scan_result = Some(result);
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> OrbFdwResult<Option<()>> {
        if let Some(ref mut result) = self.scan_result {
            if !result.is_empty() {
                let scanned = result
                    .drain(0..1)
                    .last()
                    .map(|src_row| row.replace_with(src_row));
                return Ok(scanned);
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
                let _ = check_options_contain(&options, "object");
            }
        }
        Ok(())
    }
}
