#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pgrx::pg_test]
    fn orb_smoketest() {
        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER orb_wrapper
                         HANDLER orb_fdw_handler VALIDATOR orb_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            info!("data wrapper created");
            c.update(
                r#"CREATE SERVER my_orb_server
                         FOREIGN DATA WRAPPER orb_wrapper
                         OPTIONS (
                           api_url 'http://localhost:4242/v1',  -- orb API base URL, optional
                           orb_version '2021-08-23',  -- orb API version, optional
                           api_key 'dummy_key_here'  -- orb API Key, required
                         )"#,
                None,
                None,
            )
            .unwrap();

            c.update(
                r#"
                CREATE FOREIGN TABLE orb_customers (
                  name text,
                  type text,
                  id text
                )
                SERVER my_orb_server
                OPTIONS (
                    object 'customers'    -- Corrected object name if previously incorrect
                )
                "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT name, type, id FROM orb_customers", None, None)
                .unwrap()
                .map(|r| {
                    (
                        r.get_by_name::<&str, _>("type").unwrap().unwrap(),
                        r.get_by_name::<&str, _>("name").unwrap().unwrap(),
                        r.get_by_name::<&str, _>("id").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    ("person", "John Doe", "d40e767c-d7af-4b18-a86d-55c61f1e39a4"),
                    ("bot", "Beep Boop", "9a3b5ae0-c6e6-482d-b0e1-ed315ee6dc57")
                ]
            );

            let results = c
                .select(
                    "SELECT * FROM orb_customers WHERE id = 'd40e767c-d7af-4b18-a86d-55c61f1e39a4'",
                    None,
                    None,
                )
                .unwrap()
                .map(|r| {
                    (
                        r.get_by_name::<&str, _>("type").unwrap().unwrap(),
                        r.get_by_name::<&str, _>("name").unwrap().unwrap(),
                        r.get_by_name::<&str, _>("id").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![("person", "John Doe", "d40e767c-d7af-4b18-a86d-55c61f1e39a4")]
            );
        });
    }
}
