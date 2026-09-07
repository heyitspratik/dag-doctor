-- The drift itself: an upstream system renames a column that a downstream query still
-- selects by its old name. Applied by the schema_drift_orders DAG rather than at seed
-- time, so the warehouse starts healthy and the failure is something the agent watches
-- happen.
--
-- Conditional so the DAG stays re-runnable: on a second trigger the rename is a no-op and
-- the downstream task fails in exactly the same way.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'orders' AND column_name = 'customer_id'
    ) THEN
        ALTER TABLE raw.orders RENAME COLUMN customer_id TO customer_uuid;
    END IF;
END
$$;
