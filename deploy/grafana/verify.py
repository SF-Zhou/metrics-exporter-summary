#!/usr/bin/env python3
"""Verify dashboard SQL against a disposable loopback ClickHouse.

Optionally launch an already downloaded Grafana + signed ClickHouse plugin to
verify provisioning, API import, plugin macro expansion, and result frames.
No telemetry, external downloads, or public listeners are enabled by this script.
"""
import argparse
import base64
import copy
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parent
DASHBOARD = json.loads((ROOT / "dashboard.json").read_text())
HOST_A = "node-a's\\fixture"
HOST_B = "node-b"
HISTOGRAM = "latency's\\nanoseconds"
COUNTER = "requests"
GAUGE = "connections"
LABEL_NAMES = ['host', 'pod', 'instance', 'tag', 'thread', 'uid', 'statusCode', 'mount_name', 'io']
LABELS = dict(host=HOST_A, pod="pod-a", instance="process", tag="read's\\tag", thread="1",
              uid="uid-a", statusCode="200", mount_name="/mnt/data", io="read")
COUNTER_VARIANTS = [({}, 5), ({"uid": "uid-b"}, 7), ({"pod": "pod-b"}, 11),
                    ({"instance": "process-b"}, 13), ({"tag": "write"}, 17),
                    ({"host": HOST_B}, 19), ({"thread": "2"}, 23),
                    ({"statusCode": "500"}, 29), ({"mount_name": "/mnt/other"}, 31),
                    ({"io": "write"}, 37)]
VARIABLES = {"host": [HOST_A, HOST_B], "metric": [HISTOGRAM],
             "counter_metric": [COUNTER], "gauge_metric": [GAUGE]}
VARIABLES = {key: [value.encode().hex().upper() for value in values] for key, values in VARIABLES.items()}
BASE_MS = int(time.time() // 60) * 60000 - 120000
FROM_MS, TO_MS = BASE_MS, BASE_MS + 60000
INTERVAL_MS = 10000
MAX_POINTS = 50000
SCALE_START_MS = BASE_MS - 26 * 60 * 60 * 1000


def request(url, payload=None, credentials=None, method=None, headers=None):
    headers = dict(headers or {})
    if credentials:
        headers["Authorization"] = "Basic " + base64.b64encode((':'.join(credentials)).encode()).decode()
    req = urllib.request.Request(url, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=35) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        # Failures can include SQL; fixture SQL contains no reusable secret.
        reason = error.read().decode(errors="replace")[:4000]
        raise RuntimeError(f"HTTP {error.code}: {reason}") from None


def quote(value):
    # Grafana's explicit :sqlstring formatter doubles single quotes.
    return "'" + value.replace("'", "''") + "'"


def interpolate(sql, variables=None):
    variables = VARIABLES if variables is None else variables
    return re.sub(r"\$\{(\w+):sqlstring\}", lambda match: ','.join(map(quote, variables[match[1]])), sql)


def expand(sql, variables=None, from_ms=FROM_MS, to_ms=TO_MS, interval_ms=INTERVAL_MS):
    sql = interpolate(sql, variables)
    sql = re.sub(r"\$__timeFilter_ms\((\w+)\)",
                 lambda m: f"{m[1]} >= fromUnixTimestamp64Milli({from_ms}) AND {m[1]} <= fromUnixTimestamp64Milli({to_ms})", sql)
    sql = re.sub(r"\$__timeInterval_ms\((\w+)\)",
                 lambda m: f"toStartOfInterval(toDateTime64({m[1]}, 3), INTERVAL {interval_ms} millisecond)", sql)
    sql = sql.replace("$__fromTime_ms", f"fromUnixTimestamp64Milli({from_ms})")
    sql = sql.replace("$__toTime_ms", f"fromUnixTimestamp64Milli({to_ms})")
    sql = sql.replace("$__interval_ms", str(interval_ms))
    assert "$" not in sql, "unexpanded dashboard macro or variable"
    return sql


def statements(path):
    text = re.sub(r"--[^\n]*", "", path.read_text())
    return [sql.strip() for sql in text.split(';') if sql.strip()]


def provision_fixtures(endpoint, admin, reader_password):
    def execute(sql):
        return request(endpoint, sql.encode(), admin)
    for sql in statements(ROOT.parent / "clickhouse/schema.sql"):
        execute(sql)
    def row(second, **labels):
        stamp = BASE_MS // 1000 + second
        return dict({**LABELS, **labels}, TIMESTAMP=stamp)

    first = dict(row(1), metricName=HISTOGRAM, count=2, mean=1_500_000,
                 min=1_000_000, p50=1_000_000, p90=1_900_000, p95=1_950_000, p99=1_990_000, max=2_000_000)
    distributions = [first, first,
                     dict(row(2), metricName=HISTOGRAM, count=3, mean=4_000_000,
                          min=3_000_000, p50=4_000_000, p90=4_800_000, p95=4_900_000, p99=5_000_000, max=5_000_000),
                     dict(row(1, uid="uid-b"), metricName=HISTOGRAM, count=4, mean=10_000_000,
                          min=10_000_000, p50=10_000_000, p90=10_000_000, p95=10_000_000, p99=10_000_000, max=10_000_000),
                     dict(row(1, host=HOST_B), metricName=HISTOGRAM, count=1, mean=10_000_000,
                          min=10_000_000, p50=10_000_000, p90=10_000_000, p95=10_000_000, p99=10_000_000, max=10_000_000)]
    counters = []
    for labels, increments in COUNTER_VARIANTS:
        # Only end timestamps remain. Missing observations cannot be recovered,
        # and rates use a fixed bucket width, not an inferred collection span.
        counters.extend([dict(row(1, **labels), metricName=COUNTER, val=2 * increments),
                         dict(row(4, **labels), metricName=COUNTER, val=3 * increments),
                         dict(row(5, **labels), metricName=COUNTER, val=0),
                         dict(row(6, **labels), metricName=COUNTER, val=7)])
    counters.append(counters[0])  # Physical replay is visible: no row identity.
    counters.append(dict(row(4), metricName=COUNTER, val=3))
    counters.append(dict(row(21), metricName=COUNTER, val=4))  # No rows in the middle 10s bucket.
    # Gauge and Counter share one table. Metric names provide the distinction.
    gauges = [dict(row(1), metricName=GAUGE, val=7),
              dict(row(2), metricName=GAUGE, val=9),
              dict(row(2), metricName=GAUGE, val=0),  # Same-second tie: no exact latest value.
              dict(row(2, uid="uid-b"), metricName=GAUGE, val=100)]
    for table, rows in [("distributions", distributions), ("counters", counters + gauges)]:
        execute(f"INSERT INTO metrics_summary.{table} FORMAT JSONEachRow\n" + '\n'.join(json.dumps(r) for r in rows))
    for sql in statements(ROOT / "readonly-user.sql"):
        # Bind the ephemeral password as a query parameter, preserving the shipped SQL template.
        params=urllib.parse.urlencode({'param_grafana_password':reader_password}) if '{grafana_password:String}' in sql else ''
        request(endpoint+('?' + params if params else ''),sql.encode(),admin)


def verify_sql(endpoint, reader):
    def query(sql):
        return json.loads(request(endpoint, (sql + ' FORMAT JSON').encode(), reader))['data']

    # Extra columns from the supplied schema are unused and retain their defaults.
    assert query("SELECT countIf(method != '') AS used FROM metrics_summary.distributions")[0]['used'] in (0, '0')
    assert query("SELECT countIf(type != '') AS used FROM metrics_summary.counters")[0]['used'] in (0, '0')
    stored = query("SELECT count() AS windows, sum(val) AS increments FROM metrics_summary.counters WHERE metricName = 'requests'")[0]
    assert int(stored['windows']) == 43 and int(stored['increments']) == 1047, stored
    panel_data = {}
    for variable in DASHBOARD['templating']['list']:
        assert query(expand(variable['query']['rawSql'])), variable['name']
    for panel in DASHBOARD['panels']:
        for target in panel.get('targets', []):
            sql = target['rawSql']
            tables = re.findall(r'FROM\s+metrics_summary\.(\w+)', sql)
            assert tables and set(tables) <= {'counters', 'distributions'}, panel['title']
            assert 'FINAL' not in sql and 'source_session_id' not in sql and 'duration_ns' not in sql
            rows = query(expand(sql))
            assert rows, panel['title']
            panel_data[panel['id']] = rows

    def by_series(panel):
        rows = panel_data[panel]
        result = {tuple(json.loads(row['series'])): row for row in rows}
        assert len(result) == len(rows)
        return result

    def histogram_by_labels(panel):
        return {(identity[1], identity[6]): row for identity, row in by_series(panel).items()}

    primary = (HOST_A, 'uid-a')
    assert {key: int(row['samples']) for key, row in histogram_by_labels(2).items()} == {
        primary: 7, (HOST_A, 'uid-b'): 4, (HOST_B, 'uid-a'): 1}
    assert abs(histogram_by_labels(3)[primary]['weighted_mean'] - 18_000_000 / 7) < 1e-6
    assert abs(histogram_by_labels(4)[primary]['mean_local_window_p99'] - 8_980_000 / 3) < 1e-6
    assert histogram_by_labels(5)[primary]['worst_local_window_p99'] == 5_000_000
    assert histogram_by_labels(6)[primary]['observed_max'] == 5_000_000
    coverage = {(row['host'], row['uid']): row for row in panel_data[7]}
    assert len(coverage) == 3 and int(coverage[primary]['stored_rows']) == 3
    assert coverage[primary]['observed_min'] == 1_000_000
    assert abs(coverage[primary]['mean_local_window_p95'] - 8_800_000 / 3) < 1e-6
    assert coverage[primary]['worst_local_window_p95'] == 4_900_000
    assert all(row['tag'] == LABELS['tag'] for row in coverage.values())

    observed = {}
    for row in panel_data[8]:
        observed.setdefault(tuple(json.loads(row['series'])), []).append(row)
    assert len(observed) == 10
    for labels, increments in COUNTER_VARIANTS:
        identity = (COUNTER, *({**LABELS, **labels}).values())
        rows = observed[identity]
        expected = [(5 * increments + 7) / 10]
        if not labels:
            expected = [4.5, 0.4]  # Duplicate and unidentifiable extra sample both count.
            assert rows[0]['time'] != rows[1]['time']
            assert len(rows) == 2  # The middle empty bucket is not manufactured.
        assert [row['observed_rate_per_second'] for row in rows] == expected, rows
    for panel in (9, 10):
        gauge_rows = list(by_series(panel).values()) if panel == 9 else panel_data[panel]
        baseline = next(row for row in gauge_rows if row['observed_min'] == 0)
        assert baseline['observed_max'] == 9
        assert abs(baseline['observed_mean'] - 16 / 3) < 1e-12
        assert len(gauge_rows) == 2  # No fictional source sessions distinguish the tie.

    request(endpoint, b'SELECT 1 SETTINGS max_execution_time=20', reader)
    try:
        request(endpoint, b'CREATE TABLE metrics_summary.forbidden_probe (value UInt8) ENGINE=Memory', reader)
    except RuntimeError:
        pass
    else:
        raise AssertionError('Grafana reader unexpectedly has write permission')
    print('Verified all 4 variable queries and 9 panel queries: nine independent flat labels, visible repeated rows, nanosecond distribution values/min/p95, adaptive Counter rates (10-second minimum), integer Gauge same-second ranges, unused method/type defaults, and readonly permissions.')


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def scale_variables(hosts, period):
    values = {'host': [f'scale-{host:04d}' for host in range(hosts)],
              'metric': [f'scale.histogram.{period}'],
              'counter_metric': [f'scale.counter.{period}'],
              'gauge_metric': [f'scale.gauge.{period}']}
    return {key: [value.encode().hex().upper() for value in items] for key, items in values.items()}


def scale_cases():
    for period, seconds, cadence in [('1h', 3600, 10), ('24h', 86400, 60)]:
        for hosts in (200, 500, 1000):
            yield period, seconds, cadence, hosts


def provision_scale_fixtures(endpoint, admin):
    # More than five million generated rows, limited to this disposable server.
    # Counter has two tag values per host: sizing from host count alone is wrong.
    for period, seconds, cadence in [('1h', 3600, 10), ('24h', 86400, 60)]:
        one_variant_rows = (seconds // cadence + 1) * 1000
        for table, kind, value_columns, values in [
            ('counters', 'counter', 'val', '100'),
            ('counters', 'gauge', 'val', '7'),
            ('distributions', 'histogram', 'count, mean, min, p50, p90, p95, p99, max',
             '2, 5000, 1000, 3000, 5000, 7000, 8000, 9000')]:
            variants = 2 if kind == 'counter' else 1
            sql = f"""INSERT INTO metrics_summary.{table} (TIMESTAMP, metricName, host, tag, {value_columns})
SELECT toDateTime({SCALE_START_MS // 1000} + intDiv(number % {one_variant_rows}, 1000) * {cadence}),
       'scale.{kind}.{period}', concat('scale-', leftPad(toString(number % 1000), 4, '0')),
       concat('variant-', toString(intDiv(number, {one_variant_rows}))), {values}
FROM numbers({one_variant_rows * variants})"""
            request(endpoint, sql.encode(), admin)


def bucket_seconds(series, value_columns, span_ms, interval_ms):
    slots = max(1, MAX_POINTS // max(series * value_columns, 1))
    return max(10, (interval_ms + 999) // 1000, span_ms // (slots * 1000) + 1)


def verify_scale_sql(endpoint, reader):
    def query(sql):
        result = json.loads(request(endpoint, (sql + ' FORMAT JSONCompact').encode(), reader))
        names = [field['name'] for field in result['meta']]
        return [dict(zip(names, row)) for row in result['data']]

    panels = [panel for panel in DASHBOARD['panels'] if panel['type'] == 'timeseries']
    assert len(panels) == 7
    settings = query("SELECT name, value FROM system.settings WHERE name IN ('max_result_rows', 'result_overflow_mode')")
    assert {row['name']: row['value'] for row in settings} == {
        'max_result_rows': '100000', 'result_overflow_mode': 'throw'}
    for period, seconds, cadence, hosts in scale_cases():
        variables = scale_variables(hosts, period)
        for panel in panels:
            sql = expand(panel['targets'][0]['rawSql'], variables, SCALE_START_MS,
                         SCALE_START_MS + seconds * 1000, interval_ms=1000)
            rows = query(sql)  # Execute the actual panel result under the reader's cap.
            series_count = hosts * (2 if panel['id'] == 8 else 1)
            columns = 3 if panel['id'] == 9 else 1
            assert 0 < len(rows) * columns <= MAX_POINTS, (period, hosts, panel['id'], len(rows))
            identities = {tuple(json.loads(row['series'])) for row in rows}
            assert len(identities) == series_count, (period, hosts, panel['id'], len(identities))
            assert {identity[1] for identity in identities} == {f'scale-{host:04d}' for host in range(hosts)}
            width = bucket_seconds(series_count, columns, seconds * 1000, 1000)
            counts = {}
            for second in range(0, seconds + 1, cadence):
                index = second // width
                counts[index] = counts.get(index, 0) + 1
            times = sorted({row['time'] for row in rows})
            assert len(times) == len(counts), (period, hosts, panel['id'], len(times), width)
            expected_counts = dict(zip(times, [counts[index] for index in sorted(counts)]))
            assert len(rows) == len(times) * series_count  # No silent series/point truncation.
            for row in rows:
                if panel['id'] == 2:
                    assert float(row['samples']) == 2 * expected_counts[row['time']]
                elif panel['id'] == 8:
                    expected = 100 * expected_counts[row['time']] / width
                    assert abs(row['observed_rate_per_second'] - expected) < 1e-10, (row, width, expected)
                elif panel['id'] == 9:
                    assert [row[name] for name in ('observed_mean', 'observed_min', 'observed_max')] == [7, 7, 7]
            if panel['id'] == 8:
                assert {identity[4] for identity in identities} == {'variant-0', 'variant-1'}
        print(f'Adaptive point budget: {hosts} hosts × {period}, all seven time-series panels passed; '
              'both Counter tag series retained, Gauge counts all three values, inclusive right endpoint preserved.')

    # Empty scope, zero-length window, non-second-aligned start, inclusive right
    # endpoint, and a fractional-second Grafana interval (round upward).
    for from_offset, to_offset, interval in [(0, 0, 1), (0, 1000, 1),
                                            (999, 1000, 1), (0, 60000, 15500)]:
        for panel in panels:
            rows = query(expand(panel['targets'][0]['rawSql'], VARIABLES,
                                BASE_MS + from_offset, BASE_MS + to_offset, interval))
            assert len(rows) * (3 if panel['id'] == 9 else 1) <= MAX_POINTS
            if to_offset == 0:
                assert rows == []
            if from_offset == 999:
                assert rows, panel['title']
            if panel['id'] == 8 and to_offset == 60000:
                by_identity = {}
                for row in rows:
                    by_identity.setdefault(tuple(json.loads(row['series'])), []).append(row)
                assert [row['observed_rate_per_second'] for row in by_identity[(COUNTER, *LABELS.values())]] == [45 / 16, 4 / 16]
    for panel in panels:
        rows = query(expand(panel['targets'][0]['rawSql'], VARIABLES, BASE_MS + 1000, BASE_MS + 1000, 1))
        assert rows, ('single populated timestamp', panel['title'])
    print('Empty, sub-second, single-timestamp, exact endpoint, and 15.5-second interval cases passed.')


def verify_series_budget_boundary(endpoint, admin, reader):
    for panel_id, metric, allowed_series in [(8, 'counter', MAX_POINTS), (9, 'gauge', MAX_POINTS // 3)]:
        sql = f"""INSERT INTO metrics_summary.counters (TIMESTAMP, metricName, host, uid, val)
SELECT toDateTime({SCALE_START_MS // 1000} + if(number = {allowed_series}, 1, 0)),
       'budget.{metric}', 'budget-host', toString(number), 100
FROM numbers({allowed_series + 1})"""
        request(endpoint, sql.encode(), admin)
        panel = next(panel for panel in DASHBOARD['panels'] if panel['id'] == panel_id)
        variables = dict(VARIABLES, host=['budget-host'.encode().hex().upper()],
                         **{f'{metric}_metric': [f'budget.{metric}'.encode().hex().upper()]})
        raw = panel['targets'][0]['rawSql']
        # Exactly one point per complete label identity is allowed. The next
        # timestamp adds one identity and must fail, never truncate or collapse it.
        rendered = expand(raw, variables, SCALE_START_MS, SCALE_START_MS, 1)
        rows = json.loads(request(endpoint, (rendered + ' FORMAT JSONCompact').encode(), reader))['data']
        assert len(rows) == allowed_series, (metric, len(rows), allowed_series)
        rendered = expand(raw, variables, SCALE_START_MS, SCALE_START_MS + 1000, 1)
        try:
            request(endpoint, (rendered + ' FORMAT JSONCompact').encode(), reader)
        except RuntimeError as error:
            assert 'Selected series exceed the 50000 point budget' in str(error), str(error)
        else:
            raise AssertionError(f'{metric}: excessive series must produce an explicit query error')
    print('Exact 50,000-point budget boundaries passed; excess Counter/Gauge identities fail explicitly.')

    # Model a newly appearing series between the sizing scan and main scan by
    # deliberately underestimating cardinality. The final, post-grouping window
    # guard must reject the actual point count before returning any truncated data.
    panel = next(panel for panel in DASHBOARD['panels'] if panel['id'] == 8)
    underestimated = re.sub(r'\(SELECT uniqExact\(tuple\(.*?\) AS series_count',
                           'toUInt64(1) AS series_count', panel['targets'][0]['rawSql'], flags=re.S)
    assert underestimated != panel['targets'][0]['rawSql']
    rendered = expand(underestimated, scale_variables(200, '1h'), SCALE_START_MS,
                      SCALE_START_MS + 3600000, 1000)
    try:
        request(endpoint, (rendered + ' FORMAT JSONCompact').encode(), reader)
    except RuntimeError as error:
        assert 'Selected series changed during the query' in str(error), str(error)
    else:
        raise AssertionError('The actual output guard must catch cardinality changes between scans')
    print('Actual-output guard rejects underestimated/concurrently changing series cardinality.')


def verify_grafana(home, plugin_zip, endpoint, reader):
    with tempfile.TemporaryDirectory(prefix='metrics-grafana-verify-') as temporary:
        temporary = Path(temporary)
        plugins = temporary / 'plugins'
        with zipfile.ZipFile(plugin_zip) as archive:
            archive.extractall(plugins)
            plugin=json.loads((plugins/'grafana-clickhouse-datasource/plugin.json').read_text())
            assert plugin['info']['version']=='4.20.0', 'verification is pinned to official plugin 4.20.0'
            for item in archive.infolist():
                mode=(item.external_attr>>16)&0o777
                if mode:(plugins/item.filename).chmod(mode)
        provisioning = temporary / 'provisioning'
        shutil.copytree(ROOT / 'provisioning', provisioning)
        dashboard_dir = temporary / 'dashboards'
        dashboard_dir.mkdir()
        shutil.copy(ROOT / 'dashboard.json', dashboard_dir)
        dashboard_provision = provisioning / 'dashboards/metrics-summary.yml'
        dashboard_provision.write_text(dashboard_provision.read_text().replace('/var/lib/grafana/dashboards/metrics-summary', str(dashboard_dir)))
        parsed = urllib.parse.urlsplit(endpoint)
        datasource = provisioning / 'datasources/metrics-summary.yml'
        datasource.write_text(datasource.read_text().replace('port: 8443', f'port: {parsed.port}').replace('secure: true', 'secure: false'))
        port = free_port()
        password = secrets.token_urlsafe(24)
        env = dict(os.environ, GF_PATHS_HOME=str(home), GF_PATHS_DATA=str(temporary / 'data'),
                   GF_PATHS_LOGS=str(temporary / 'logs'), GF_PATHS_PLUGINS=str(plugins),
                   GF_PATHS_PROVISIONING=str(provisioning), GF_SERVER_HTTP_ADDR='127.0.0.1',
                   GF_SERVER_HTTP_PORT=str(port), GF_SECURITY_ADMIN_USER='admin',
                   GF_SECURITY_ADMIN_PASSWORD=password, GF_SECURITY_SECRET_KEY=secrets.token_urlsafe(32),
                   GF_AUTH_ANONYMOUS_ENABLED='false', GF_ANALYTICS_REPORTING_ENABLED='false',
                   GF_ANALYTICS_CHECK_FOR_UPDATES='false', GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES='false',
                   GF_NEWS_NEWS_FEED_ENABLED='false', GF_PLUGINS_PREINSTALL_DISABLED='true',
                   GF_PLUGINS_PUBLIC_KEY_RETRIEVAL_DISABLED='true', GF_SECURITY_DISABLE_GRAVATAR='true',
                   GF_LOG_LEVEL='warn', GRAFANA_CLICKHOUSE_HOST=parsed.hostname,
                   GRAFANA_CLICKHOUSE_PASSWORD=reader[1])
        log_path = temporary / 'grafana.log'
        with log_path.open('wb') as log:
            process = subprocess.Popen([str(home / 'bin/grafana'), 'server', '--homepath', str(home)], env=env, stdout=log, stderr=subprocess.STDOUT)
        base = f'http://127.0.0.1:{port}'
        auth = ('admin', password)
        try:
            until = time.monotonic() + 60
            while True:
                try:
                    health = json.loads(request(base + '/api/health'))
                    break
                except (OSError, RuntimeError):
                    if process.poll() is not None or time.monotonic() >= until:
                        raise RuntimeError('Grafana failed to become ready: ' + log_path.read_text()[-8000:])
                    time.sleep(0.2)
            datasource_health = json.loads(request(base + '/api/datasources/uid/metrics-summary-clickhouse/health', credentials=auth))
            assert datasource_health['status'] == 'OK', datasource_health
            provisioned = json.loads(request(base + '/api/dashboards/uid/metrics-summary-v1', credentials=auth))
            assert provisioned['dashboard']['title'] == DASHBOARD['title']
            imported = copy.deepcopy(DASHBOARD)
            imported.update(uid='metrics-summary-api-verification', editable=True, version=0)
            result = json.loads(request(base + '/api/dashboards/db', json.dumps({'dashboard': imported, 'overwrite': True}).encode(), auth, headers={'Content-Type': 'application/json'}))
            assert result['status'] == 'success', result
            queries=[('variable '+variable['name'],variable['query']) for variable in DASHBOARD['templating']['list']]
            queries.extend((panel['title'],target) for panel in DASHBOARD['panels'] for target in panel.get('targets',[]))
            for title,original in queries:
                target = dict(original, rawSql=interpolate(original['rawSql']), intervalMs=INTERVAL_MS, maxDataPoints=1000)
                body = {'queries': [target], 'from': str(FROM_MS), 'to': str(TO_MS)}
                result = json.loads(request(base + '/api/ds/query', json.dumps(body).encode(), auth, headers={'Content-Type': 'application/json'}))['results']['A']
                assert not result.get('error'), (title, result)
                assert result.get('frames'), (title, result)
                assert any(any(len(column)>0 for column in frame.get('data', {}).get('values',[])) for frame in result['frames']), title
                if title=='variable host':
                    assert HOST_A in result['frames'][0]['data']['values'][1]
                if title=='Counter observed rate per adaptive bucket':
                    rates = [value for frame in result['frames']
                             for field, column in zip(frame['schema']['fields'], frame['data']['values'])
                             if field['name']=='observed_rate_per_second' for value in column]
                    finite = [rate for rate in rates if rate is not None]
                    expected = [4.5, 0.4] + [(5 * increments + 7) / 10 for labels, increments in COUNTER_VARIANTS if labels]
                    assert sorted(finite) == sorted(expected), rates
            # Exercise real plugin expansion with numeric $__interval_ms and the
            # generated per-query scalar subquery, including actual large frames.
            scale_queries = 0
            for period, seconds, cadence, hosts in scale_cases():
                for panel_id in (2, 8, 9):
                    panel = next(panel for panel in DASHBOARD['panels'] if panel['id'] == panel_id)
                    original = panel['targets'][0]
                    target = dict(original, rawSql=interpolate(original['rawSql'], scale_variables(hosts, period)),
                                  intervalMs=1000, maxDataPoints=1000)
                    body = {'queries': [target], 'from': str(SCALE_START_MS),
                            'to': str(SCALE_START_MS + seconds * 1000)}
                    result = json.loads(request(base + '/api/ds/query', json.dumps(body).encode(), auth,
                                                headers={'Content-Type': 'application/json'}))['results']['A']
                    assert not result.get('error'), (period, hosts, panel_id, result)
                    numeric = [(field, values) for frame in result['frames']
                               for field, values in zip(frame['schema']['fields'], frame['data']['values'])
                               if field['type'] == 'number']
                    points = sum(value is not None for _, values in numeric for value in values)
                    columns = 3 if panel_id == 9 else 1
                    series_count = hosts * (2 if panel_id == 8 else 1)
                    width = bucket_seconds(series_count, columns, seconds * 1000, 1000)
                    counts = {}
                    for second in range(0, seconds + 1, cadence):
                        index = second // width
                        counts[index] = counts.get(index, 0) + 1
                    assert points == len(counts) * series_count * columns <= MAX_POINTS, (period, hosts, panel_id, points)
                    identities = {field.get('labels', {}).get('series') for field, _ in numeric}
                    assert None not in identities and len(identities) == series_count, (panel_id, len(identities), numeric[0][0])
                    if panel_id == 8:
                        for _, values in numeric:
                            observed = [value for value in values if value is not None]
                            expected = [100 * counts[index] / width for index in sorted(counts)]
                            assert len(observed) == len(expected)
                            assert all(abs(a - b) < 1e-10 for a, b in zip(observed, expected))
                    scale_queries += 1
                print(f'Real Grafana plugin scale frames: {hosts} hosts × {period} passed.')
            print(f"Grafana {health['version']} + official ClickHouse plugin 4.20.0: datasource health, file provisioning, dashboard API import, {len(queries)} fixture and {scale_queries} scale macro/result-frame queries passed. Browser rendering was not tested.")
        except Exception:
            print('Grafana verification log tail:', log_path.read_text()[-6000:])
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--disposable', action='store_true', required=True, help='required: this creates fixture data and a readonly user')
    parser.add_argument('--grafana-home', type=Path)
    parser.add_argument('--plugin-zip', type=Path)
    args = parser.parse_args()
    endpoint = os.environ['METRICS_CLICKHOUSE_URL']
    assert urllib.parse.urlsplit(endpoint).hostname in ('127.0.0.1', '::1'), 'verification only mutates a disposable loopback server'
    admin = (os.environ.get('METRICS_CLICKHOUSE_USER', 'default'), os.environ.get('METRICS_CLICKHOUSE_PASSWORD', ''))
    reader = ('grafana_summary_reader', 'Grafana$' + secrets.token_urlsafe(24))
    provision_fixtures(endpoint, admin, reader[1])
    verify_sql(endpoint, reader)
    provision_scale_fixtures(endpoint, admin)
    verify_scale_sql(endpoint, reader)
    verify_series_budget_boundary(endpoint, admin, reader)
    if args.grafana_home:
        if not args.plugin_zip:
            parser.error('--plugin-zip is required with --grafana-home')
        verify_grafana(args.grafana_home.resolve(), args.plugin_zip.resolve(), endpoint, reader)


if __name__ == '__main__':
    main()
