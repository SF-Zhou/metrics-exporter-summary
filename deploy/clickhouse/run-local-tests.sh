#!/usr/bin/env bash
set -euo pipefail
binary=${1:?Usage: run-local-tests.sh /absolute/path/to/clickhouse}
shift
test_data=$(mktemp -d /tmp/metrics-clickhouse-test.XXXXXXXX)
server_pid=
cleanup() {
    if [[ -n "$server_pid" ]]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf -- "$test_data"
}
trap cleanup EXIT
http_port=$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    print(sock.getsockname()[1])
PY
)
cat > "$test_data/config.xml" <<EOF
<clickhouse>
  <logger><level>warning</level><log>$test_data/server.log</log><errorlog>$test_data/error.log</errorlog></logger>
  <listen_host>127.0.0.1</listen_host><http_port>$http_port</http_port>
  <path>$test_data/data/</path><tmp_path>$test_data/tmp/</tmp_path>
  <user_files_path>$test_data/user_files/</user_files_path>
  <access_control_path>$test_data/access/</access_control_path>
  <max_server_memory_usage>1073741824</max_server_memory_usage>
  <!-- This test process can share a cgroup with unrelated build processes. -->
  <memory_worker_use_cgroup>false</memory_worker_use_cgroup>
  <mark_cache_size>33554432</mark_cache_size><uncompressed_cache_size>0</uncompressed_cache_size>
  <max_thread_pool_size>128</max_thread_pool_size>
  <background_pool_size>4</background_pool_size>
  <background_schedule_pool_size>4</background_schedule_pool_size>
  <background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size>
  <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
  <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <merge_tree><number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_lower_max_size_of_merge>2</number_of_free_entries_in_pool_to_lower_max_size_of_merge><number_of_free_entries_in_pool_to_execute_optimize_entire_partition>2</number_of_free_entries_in_pool_to_execute_optimize_entire_partition></merge_tree>
  <profiles><default><max_threads>2</max_threads><max_memory_usage>536870912</max_memory_usage><async_insert_busy_timeout_ms>20</async_insert_busy_timeout_ms></default></profiles>
  <users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>
  <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
</clickhouse>
EOF
"$binary" server --config-file="$test_data/config.xml" > "$test_data/stdout.log" 2>&1 &
server_pid=$!
ready=0
for _ in $(seq 1 100); do
    if curl --silent --fail --max-time 1 "http://127.0.0.1:$http_port/ping" > /dev/null; then ready=1; break; fi
    if ! kill -0 "$server_pid" 2>/dev/null; then break; fi
    sleep 0.2
done
if [[ "$ready" != 1 ]]; then
    cat "$test_data/stdout.log" "$test_data/error.log" 2>/dev/null || true
    exit 1
fi
export METRICS_CLICKHOUSE_URL="http://127.0.0.1:$http_port"
export METRICS_CLICKHOUSE_USER=default
export METRICS_CLICKHOUSE_PASSWORD=
if [[ $# -gt 0 ]]; then
    "$@"
else
    cargo test -p metrics-summary-sink-clickhouse --test real_clickhouse -- --ignored --nocapture
    cargo test -p metrics-summary-collector --features tcp --test real_clickhouse -- --ignored --nocapture
fi
