#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 SIMPLE_NAIVE|SIMPLE_BIN|PGM_INDEX|CHALAMET" >&2
  exit 2
fi

protocol="$1"
case "$protocol" in
  SIMPLE_NAIVE|SIMPLE_BIN|PGM_INDEX|CHALAMET) ;;
  *) echo "unsupported protocol: $protocol" >&2; exit 2 ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
upstream="$repo_root/target/pir-research/mpc4j-1.1.3-beta"
result_root="$repo_root/target/pir-research/kpir-artifact-results/${protocol,,}"
template="$repo_root/tools/pir-poc/research/kpir_artifact/defra-n18-l768.conf"
jar="$upstream/mpc4j-s2pc-pir/target/mpc4j-s2pc-pir-1.1.3-SNAPSHOT-jar-with-dependencies.jar"

if [[ ! -f "$jar" ]]; then
  echo "missing artifact jar: $jar" >&2
  exit 1
fi

mkdir -p "$result_root"
config="$result_root/run.conf"
sed "s/^single_cp_ks_pir_pto_name = .*/single_cp_ks_pir_pto_name = $protocol/" \
  "$template" > "$config"

cd "$upstream"
rm -f temp/SINGLE_CP_KS_PIR_"$protocol"_defra_n18_l768_q100_768_*.output

/usr/bin/time -v -o "$result_root/server.time" \
  java -Xms128m -Xmx5g -jar "$jar" "$config" server \
  > "$result_root/server.log" 2>&1 &
server_time_pid=$!

cleanup() {
  server_java_pid="$(pgrep -P "$server_time_pid" 2>/dev/null || true)"
  if [[ -n "$server_java_pid" ]]; then
    kill "$server_java_pid" 2>/dev/null || true
  fi
  if kill -0 "$server_time_pid" 2>/dev/null; then
    kill "$server_time_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 3600); do
  if grep -q "ready for run" "$result_root/server.log"; then
    break
  fi
  if ! kill -0 "$server_time_pid" 2>/dev/null; then
    echo "server exited before becoming ready" >&2
    wait "$server_time_pid"
  fi
  sleep 0.1
done
grep -q "ready for run" "$result_root/server.log"

/usr/bin/time -v -o "$result_root/client.time" \
  java -Xms128m -Xmx2g -jar "$jar" "$config" client \
  > "$result_root/client.log" 2>&1
wait "$server_time_pid"
trap - EXIT

cp temp/SINGLE_CP_KS_PIR_"$protocol"_defra_n18_l768_q100_768_1_*.output \
  "$result_root/server.output"
cp temp/SINGLE_CP_KS_PIR_"$protocol"_defra_n18_l768_q100_768_2_*.output \
  "$result_root/client.output"

echo "$result_root"
