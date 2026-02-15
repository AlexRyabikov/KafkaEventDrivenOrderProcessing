CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY,
  customer_email TEXT NOT NULL,
  item_sku TEXT NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
  status TEXT NOT NULL,
  created_at_utc TIMESTAMPTZ NOT NULL,
  updated_at_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS order_steps (
  order_id TEXT PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
  payment_status TEXT NOT NULL DEFAULT 'PENDING',
  inventory_status TEXT NOT NULL DEFAULT 'PENDING',
  updated_at_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
  service_name TEXT NOT NULL,
  event_id TEXT NOT NULL,
  processed_at_utc TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (service_name, event_id)
);
