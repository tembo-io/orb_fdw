## Orb_fdw

This is a simple open-source data wrapper that bridges the gap between your Postgres database and [Orb](https://www.withorb.com/) a leading usage-based billing solution.

### Pre-requisistes

- have the v0.0.1 of `orb_fdw` extension enabled in your instance

Create the foreign data wrapper:

```
create foreign data wrapper orb_wrapper
  handler orb_fdw_handler
  validator orb_fdw_validator;
```

Connect to clerk using your credentials:

```
create server my_orb_server
  foreign data wrapper orb_wrapper
  options (
    api_key '<orb secret Key>')
```

Create Foreign Table:

### Customers table

This table will store information about the users.

```
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

```
create foreign table orb_subscriptions (
    subscription_id text,
    status text,
    plan text,
    started_at bigint,
    end_date bigint
)
server my_orb_server
options (
  object 'subscriptions'
);
```