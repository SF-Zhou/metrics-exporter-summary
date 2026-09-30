-- A counter contains interval increments; select a metric known to be a counter.
-- Missing batches lose increments. Retries may duplicate rows: this schema has no
-- stored identity with which to deduplicate. Per-row duration is not stored, so
-- a counter rate requires an externally specified reporting interval.
SELECT TIMESTAMP, host, pod, instance, tag, thread, uid, statusCode, mount_name, io, val
FROM metrics_summary.counters
WHERE metricName = 'requests_total'
ORDER BY TIMESTAMP;

-- A gauge contains the current signed integer snapshot. Counter and gauge rows
-- share counters; metricName must determine the instrument's interpretation.
SELECT TIMESTAMP, host, pod, instance, val
FROM metrics_summary.counters
WHERE metricName = 'queue_depth'
ORDER BY TIMESTAMP;

-- Weighted means can be combined. Quantiles describe individual source windows;
-- do not average quantiles and call the result a combined percentile.
SELECT metricName, host,
       sum(count) AS sample_count,
       sum(count * mean) / nullIf(sum(count), 0) AS weighted_mean,
       min(min) AS minimum, max(max) AS maximum
FROM metrics_summary.distributions
GROUP BY metricName, host;

SELECT TIMESTAMP, metricName, host, pod, instance, count, mean, min, max, p50, p90, p95, p99
FROM metrics_summary.distributions
ORDER BY TIMESTAMP, metricName;
