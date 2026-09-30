-- Run once as an access-management administrator. Pass grafana_password as a
-- query parameter from a secret manager, not as a committed SQL literal.
CREATE SETTINGS PROFILE IF NOT EXISTS grafana_summary_reader_profile
SETTINGS readonly = 1,
         max_execution_time = 30 MIN 1 MAX 60 CHANGEABLE_IN_READONLY,
         max_memory_usage = 536870912,
         max_result_rows = 100000,
         result_overflow_mode = 'throw';
CREATE USER IF NOT EXISTS grafana_summary_reader
IDENTIFIED WITH sha256_password BY {grafana_password:String}
SETTINGS PROFILE grafana_summary_reader_profile;
GRANT SELECT ON metrics_summary.distributions TO grafana_summary_reader;
GRANT SELECT ON metrics_summary.counters TO grafana_summary_reader;
-- No INSERT, ALTER, DROP, or access-management privileges are granted.
