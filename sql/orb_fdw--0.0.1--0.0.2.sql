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