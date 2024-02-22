DROP TRIGGER IF EXISTS trg_schedule_config_updated_at ON schedule_config;
DROP TRIGGER IF EXISTS trg_task_executions_updated_at ON task_executions;
DROP TRIGGER IF EXISTS trg_pipeline_runs_updated_at ON pipeline_runs;
DROP TRIGGER IF EXISTS trg_pipeline_config_updated_at ON pipeline_config;

DROP FUNCTION IF EXISTS update_updated_at_column();

DROP INDEX IF EXISTS idx_schedule_config_next_run;
DROP INDEX IF EXISTS idx_schedule_config_enabled;
DROP INDEX IF EXISTS idx_schedule_config_status;
DROP INDEX IF EXISTS idx_schedule_config_pipeline_id;

DROP INDEX IF EXISTS idx_task_executions_worker_id;
DROP INDEX IF EXISTS idx_task_executions_scheduled_at;
DROP INDEX IF EXISTS idx_task_executions_status;
DROP INDEX IF EXISTS idx_task_executions_run_id;
DROP INDEX IF EXISTS idx_task_executions_pipeline_id;

DROP INDEX IF EXISTS idx_pipeline_runs_worker_id;
DROP INDEX IF EXISTS idx_pipeline_runs_created_at;
DROP INDEX IF EXISTS idx_pipeline_runs_status;
DROP INDEX IF EXISTS idx_pipeline_runs_pipeline_id;

DROP INDEX IF EXISTS idx_pipeline_config_created_at;
DROP INDEX IF EXISTS idx_pipeline_config_created_by;
DROP INDEX IF EXISTS idx_pipeline_config_status;

DROP TABLE IF EXISTS schedule_config;
DROP TABLE IF EXISTS task_executions;
DROP TABLE IF EXISTS pipeline_runs;
DROP TABLE IF EXISTS pipeline_config;

DROP TYPE IF EXISTS task_status;
DROP TYPE IF EXISTS pipeline_status;
