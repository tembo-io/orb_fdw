use pgrx::{pg_sys, prelude::*, warning, JsonB};
use serde_json::Value as JsonValue;
use std::{collections::HashMap, env, str::FromStr};
use supabase_wrappers::prelude::*;
use tokio::runtime::Runtime;
pg_module_magic!();
mod orb_fdw;
use crate::orb_fdw::{OrbFdwError, OrbFdwResult};
use futures::StreamExt;
use orb_billing::{
    Client as OrbClient, ClientConfig as OrbClientConfig, Customer as OrbCustomer,
    Invoice as OrbInvoice, InvoiceListParams, ListParams, Subscription as OrbSubscription,
    SubscriptionListParams,
};

// TODO: Remove all unwraps. Handle the errors
fn resp_to_rows(obj: &str, resp: &JsonValue, tgt_cols: &[Column]) -> Vec<Row> {
    match obj {
        "customers" => {
            body_to_rows(
                resp,
                "data",
                vec![
                    ("id", "user_id", "string"),
                    ("external_customer_id", "organization_id", "string"),
                    ("name", "first_name", "string"),
                    ("email", "email", "string"),
                    ("payment_provider_id", "stripe_id", "string"),
                    ("created_at", "created_at", "timestamp_iso"),
                ],
                tgt_cols,
            )
        }
        "subscriptions" => body_to_rows(
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
                    "timestamp_iso",
                ),
                (
                    "current_billing_period_end_date",
                    "end_date",
                    "timestamp_iso",
                ),
            ],
            tgt_cols,
        ),
        "invoices" => body_to_rows(
            resp,
            "data",
            vec![
                ("subscription.id", "subscription_id", "string"),
                ("customer.id", "customer_id", "string"),
                ("customer.external_customer_id", "organization_id", "string"),
                ("status", "status", "string"),
                ("invoice_date", "due_date", "timestamp_iso"),
                ("amount_due", "amount", "string"),
            ],
            tgt_cols,
        ),
        _ => {
            warning!("unsupported object: {}", obj);
            Vec::new()
        }
    }
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

fn process_data<T, E: std::fmt::Display>(data: Vec<Result<T, E>>) -> Vec<T> {
    data.into_iter()
        .filter_map(|item_result| match item_result {
            Ok(item) => Some(item),
            Err(e) => {
                warning!("Error processing item: {}", e);
                None
            }
        })
        .collect()
}

#[wrappers_fdw(
    version = "0.12.1",
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

impl ForeignDataWrapper<OrbFdwError> for OrbFdw {
    fn new(options: &HashMap<String, String>) -> OrbFdwResult<Self> {
        let token = if let Some(access_token) = options.get("api_key") {
            access_token.to_owned()
        } else {
            warning!("Cannot find api_key in options, trying environment variable");
            match env::var("ORB_API_KEY") {
                Ok(key) => key,
                Err(_) => {
                    error!("{}", OrbFdwError::ApiKeyNotFound);
                }
            }
        };
        let client_config = OrbClientConfig { api_key: token };
        let orb_client = OrbClient::new(client_config);
        let rt = match create_async_runtime() {
            Ok(runtime) => runtime,
            Err(e) => {
                error!("Failed to create async runtime: {}", e);
            }
        };
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
        let obj = match require_option("object", options) {
            Ok(object) => object,
            Err(_e) => error!(
                "{}",
                OrbFdwError::MissingRequiredOption("object".to_string())
            ),
        };
        self.scan_result = None;
        self.tgt_cols = columns.to_vec();
        let mut result = Vec::new();

        let run: Result<(), Box<dyn std::error::Error>> = self.rt.block_on(async {
            let obj_js = match obj {
                "customers" => {
                    let customers_stream = self
                        .client
                        .list_customers(&ListParams::DEFAULT.page_size(500));
                    let customers = customers_stream.collect::<Vec<_>>().await;
                    let processed_customers: Vec<OrbCustomer> = process_data(customers);

                    match serde_json::to_value(processed_customers) {
                        Ok(value) => value,
                        Err(e) => error!("{}", OrbFdwError::JsonSerializationError(e)),
                    }
                }
                "subscriptions" => {
                    let subscriptions_stream = self
                        .client
                        .list_subscriptions(&SubscriptionListParams::DEFAULT.page_size(500));
                    let subscriptions = subscriptions_stream.collect::<Vec<_>>().await;
                    let processed_subscriptions: Vec<OrbSubscription> = process_data(subscriptions);

                    match serde_json::to_value(processed_subscriptions) {
                        Ok(value) => value,
                        Err(e) => error!("{}", OrbFdwError::JsonSerializationError(e)),
                    }
                }
                "invoices" => {
                    let invoices_stream = self
                        .client
                        .list_invoices(&InvoiceListParams::DEFAULT.page_size(500));
                    let invoices = invoices_stream.collect::<Vec<_>>().await;
                    let processed_invoices: Vec<OrbInvoice> = process_data(invoices);

                    match serde_json::to_value(processed_invoices) {
                        Ok(value) => value,
                        Err(e) => error!("{}", OrbFdwError::JsonSerializationError(e)),
                    }
                }
                _ => {
                    warning!("unsupported object: {}", obj);
                    return Ok(());
                }
            };

            let mut rows = resp_to_rows(obj, &obj_js, &self.tgt_cols[..]);
            result.append(&mut rows);
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
