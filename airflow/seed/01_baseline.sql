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

-- Tables the remaining seeded scenarios read and write.
BEGIN;

-- null_explosion_customers: the downstream dimension insists on a country code, so an
-- upstream column going mostly null fails here rather than where it was caused.
CREATE TABLE IF NOT EXISTS analytics.customer_dim (
    customer_id   text PRIMARY KEY,
    country_code  text NOT NULL,
    built_at      timestamptz NOT NULL DEFAULT now()
);

-- type_coercion_failure: amounts land as text from the source system, exactly as they
-- do in real ingestion, and one row is not a number.
CREATE TABLE IF NOT EXISTS raw.payments_staging (
    payment_id  text PRIMARY KEY,
    amount      text,
    paid_at     timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.payments (
    payment_id  text PRIMARY KEY,
    amount      numeric(12, 2) NOT NULL,
    paid_at     timestamptz NOT NULL
);

INSERT INTO raw.payments_staging (payment_id, amount, paid_at)
SELECT
    'pay-' || lpad(n::text, 5, '0'),
    -- One row in fifty is not a number. This is what a source system does when a field
    -- is optional upstream and nobody told the pipeline.
    CASE WHEN n % 50 = 0 THEN 'N/A' ELSE ((n * 13) % 9000 + 100)::text END,
    now() - (n || ' hours')::interval
FROM generate_series(1, 500) AS n
ON CONFLICT (payment_id) DO NOTHING;

-- stale_partition_missing: events land daily, and the most recent day never arrived.
CREATE TABLE IF NOT EXISTS raw.events_daily (
    event_id    text PRIMARY KEY,
    event_date  date NOT NULL,
    payload     text
);

CREATE INDEX IF NOT EXISTS ix_events_daily_date ON raw.events_daily (event_date);

INSERT INTO raw.events_daily (event_id, event_date, payload)
SELECT
    'evt-' || lpad(n::text, 6, '0'),
    -- Deliberately stops at yesterday. Today's partition is the one that never landed.
    (current_date - ((n % 14) + 1))::date,
    'payload-' || n
FROM generate_series(1, 2000) AS n
ON CONFLICT (event_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.events_daily_summary (
    event_date   date PRIMARY KEY,
    event_count  bigint NOT NULL,
    built_at     timestamptz NOT NULL DEFAULT now()
);

-- upstream_dependency_failure: the partner feed is built by an upstream task that fails,
-- so this table stays empty and the task two steps downstream is the one that complains.
CREATE TABLE IF NOT EXISTS raw.partner_feed (
    partner_id  text PRIMARY KEY,
    region      text NOT NULL,
    loaded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.partner_metrics (
    partner_id    text PRIMARY KEY,
    order_count   bigint NOT NULL,
    built_at      timestamptz NOT NULL DEFAULT now()
);

-- bad_sql_join_explosion: a small dimension whose join key is not unique, which is how a
-- cartesian product gets written by accident.
CREATE TABLE IF NOT EXISTS raw.order_tags (
    order_id  text NOT NULL,
    tag       text NOT NULL
);

INSERT INTO raw.order_tags (order_id, tag)
SELECT 'ord-' || lpad(((n % 5000) + 1)::text, 6, '0'), 'tag-' || (n % 40)
FROM generate_series(1, 40000) AS n;

COMMIT;
