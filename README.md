## Orb_fdw

This is a simple open-source data wrapper that bridges the gap between your Postgres database and [Orb](https://www.withorb.com/) a leading usage-based billing solution.

[![Static Badge](https://img.shields.io/badge/%40tembo-community?logo=slack&label=slack)](https://join.slack.com/t/tembocommunity/shared_invite/zt-20dtnhcmo-pLNV7_Aobi50TdTLpfQ~EQ)
[![PGXN version](https://badge.fury.io/pg/orb_fdw.svg)](https://pgxn.org/dist/orb_fdw/)

### Pre-requisistes

- have the v0.0.3 of `orb_fdw` extension enabled in your instance

Create the foreign data wrapper:

``` sql
create foreign data wrapper orb_wrapper
  handler orb_fdw_handler
  validator orb_fdw_validator;
```

Connect to orb using your credentials:

``` sql
create server my_orb_server
  foreign data wrapper orb_wrapper
  options (
    api_key '<orb secret Key>')
```

Create Foreign Table:

### Customers table

This table will store information about the users.

``` sql
create foreign table orb_customers (
  user_id text,
  organization_id text,
  first_name text,
  email text,
  stripe_id text,
  created_at text
  )
  server my_orb_server
  options (
      object 'customers'
  );
```

### Subscriptions Table

This table will store information about the subscriptions.

``` sql
create foreign table orb_subscriptions (
    subscription_id text,
    organization_id text,
    status text,
    plan text,
    started_date text,
    end_date text
  )
  server my_orb_server
  options (
    object 'subscriptions'
  );
```

### Invoices Table

This table will store information about the subscriptions.

``` sql
create foreign table orb_invoices (
    customer_id text,
    subscription_id text,
    organization_id text,
    status text,
    due_date text,
    amount text
  )
  server my_orb_server
  options (
    object 'invoices'
  );
```