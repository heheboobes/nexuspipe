DROP TRIGGER IF EXISTS trg_webhook_configs_updated_at ON webhook_configs;
DROP FUNCTION IF EXISTS update_updated_at_column();

DROP INDEX IF EXISTS idx_webhook_logs_level;
DROP INDEX IF EXISTS idx_webhook_logs_delivery;
DROP INDEX IF EXISTS idx_webhook_logs_webhook;

DROP INDEX IF EXISTS idx_webhook_deliveries_created;
DROP INDEX IF EXISTS idx_webhook_deliveries_event;
DROP INDEX IF EXISTS idx_webhook_deliveries_retry;
DROP INDEX IF EXISTS idx_webhook_deliveries_attempt;
DROP INDEX IF EXISTS idx_webhook_deliveries_status;
DROP INDEX IF EXISTS idx_webhook_deliveries_webhook;

DROP INDEX IF EXISTS idx_webhook_configs_created_at;
DROP INDEX IF EXISTS idx_webhook_configs_events;
DROP INDEX IF EXISTS idx_webhook_configs_status;
DROP INDEX IF EXISTS idx_webhook_configs_pipeline;

DROP TABLE IF EXISTS webhook_logs CASCADE;
DROP TABLE IF EXISTS webhook_deliveries CASCADE;
DROP TABLE IF EXISTS webhook_configs CASCADE;

DROP TYPE IF EXISTS delivery_status;
DROP TYPE IF EXISTS webhook_status;
