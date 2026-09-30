-- Optional operator action; the writer never changes table TTLs.
ALTER TABLE metrics_summary.counters MODIFY TTL TIMESTAMP + toIntervalMonth(3);
ALTER TABLE metrics_summary.distributions MODIFY TTL TIMESTAMP + toIntervalMonth(3);
