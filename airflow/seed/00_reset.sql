-- Put the warehouse back to its pre-failure state, so the scenarios can be run again.
--
-- The seeded DAGs damage the data on purpose, and most of that damage is not idempotent:
-- once orders.customer_id has been renamed there is nothing left to rename, so a second
-- run produces the same failure but no fresh drift for the snapshot tool to find. Running
-- this before taking a baseline is what makes the demonstration repeatable.
BEGIN;

-- schema_drift_orders renamed this away.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'orders' AND column_name = 'customer_uuid'
    ) THEN
        ALTER TABLE raw.orders RENAME COLUMN customer_uuid TO customer_id;
    END IF;
END
$$;

-- null_explosion_customers nulled most of these.
UPDATE raw.customers
SET country_code = (ARRAY['GB', 'US', 'DE', 'IN'])[1 + (abs(hashtext(customer_id)) % 4)]
WHERE country_code IS NULL;

-- upstream_dependency_failure leaves this empty when its extract fails, which is the
-- point, so it starts empty.
TRUNCATE raw.partner_feed;

-- bad_sql_join_explosion writes this only when it unexpectedly succeeds.
DROP TABLE IF EXISTS analytics.order_tag_revenue;

COMMIT;
