-- The warehouse as it looks before anything drifts. This is the data the broken DAGs
-- read and the agent's schema and profiling tools inspect.
BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id   text PRIMARY KEY,
    email         text,
    country_code  text,
    signed_up_at  timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id      text PRIMARY KEY,
    customer_id   text NOT NULL,
    order_ts      timestamptz NOT NULL,
    amount_cents  bigint NOT NULL,
    status        text NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.orders_daily (
    order_date    date PRIMARY KEY,
    order_count   bigint NOT NULL,
    revenue_cents bigint NOT NULL,
    built_at      timestamptz NOT NULL DEFAULT now()
);

INSERT INTO raw.customers (customer_id, email, country_code, signed_up_at)
SELECT
    'cust-' || lpad(n::text, 5, '0'),
    'user' || n || '@example.com',
    (ARRAY['GB', 'US', 'DE', 'IN'])[1 + (n % 4)],
    now() - (n || ' days')::interval
FROM generate_series(1, 500) AS n
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO raw.orders (order_id, customer_id, order_ts, amount_cents, status)
SELECT
    'ord-' || lpad(n::text, 6, '0'),
    'cust-' || lpad((1 + (n % 500))::text, 5, '0'),
    now() - ((n % 30) || ' days')::interval,
    500 + (n * 37) % 45000,
    (ARRAY['placed', 'shipped', 'delivered', 'refunded'])[1 + (n % 4)]
FROM generate_series(1, 5000) AS n
ON CONFLICT (order_id) DO NOTHING;

COMMIT;
