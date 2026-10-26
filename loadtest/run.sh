#!/bin/bash
# Runs the load-test stages one after another, then writes a report.
#
#   loadtest/run.sh                    all 4 stages, 5 minutes each, 60 s pause between them
#   DURATION=1m COOLDOWN=20 loadtest/run.sh
#   STAGES="stage1" loadtest/run.sh    only some stages
#
# k6 runs on the Mac (not in the cluster), so the load generator doesn't take CPU or memory from what it measures.
# Its metrics go to Prometheus (labels testid, stage), so they show up in Grafana next to the JVM metrics.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${DURATION:-5m}
COOLDOWN=${COOLDOWN:-60}
STAGES=${STAGES:-"stage1 stage2 stage3 stage4"}
RUN=$(date +%Y%m%d-%H%M%S)
DIR=loadtest/reports/$RUN
mkdir -p "$DIR"

# stage → total searches, total ingests (spread over DURATION)
declare -A SEARCHES=([stage1]=1000 [stage2]=5000 [stage3]=50000 [stage4]=200000)
declare -A INGESTS=([stage1]=500 [stage2]=1000 [stage3]=10000 [stage4]=50000)

echo "Load test $RUN: stages [$STAGES], $DURATION each → $DIR"
for stage in $STAGES; do
  start=$(date +%s)
  K6_PROMETHEUS_RW_SERVER_URL=http://localhost:9090/api/v1/write \
  K6_PROMETHEUS_RW_TREND_STATS="p(50),p(95),p(99),max" \
    k6 run --quiet -o experimental-prometheus-rw --tag testid="$RUN" --tag stage="$stage" \
      -e STAGE="$stage" -e SEARCHES="${SEARCHES[$stage]}" -e INGESTS="${INGESTS[$stage]}" \
      -e DURATION="$DURATION" -e REPORT_DIR="$DIR" \
      loadtest/search-and-ingest.js || true # missed targets are a result, not a failure
  end=$(date +%s)
  printf '%s\t%s\t%s\t%s\t%s\n' "$stage" "${SEARCHES[$stage]}" "${INGESTS[$stage]}" "$start" "$end" >> "$DIR/stages.tsv"
  if [ "$stage" != "${STAGES##* }" ]; then
    echo "cooling down ${COOLDOWN}s (lets queues drain, so stages don't blur into each other)"
    sleep "$COOLDOWN"
  fi
done

python3 -I loadtest/report.py "$DIR"
echo "Report: $DIR/report.md"
