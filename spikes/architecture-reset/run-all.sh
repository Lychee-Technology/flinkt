#!/usr/bin/env bash
# Builds and runs every probe on every Flink line and writes the outputs to results/.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
LIBS=${SPIKE_LIBS:-$HERE/libs}; KOTLINC=${KOTLINC:-$HERE/tools/kotlinc/bin/kotlinc}
OUT=$HERE/out; R=$HERE/results; mkdir -p $OUT $R
( cd $HERE/inventory
  for l in f120 f22 f23; do java -cp "$LIBS/$l/*" Inventory.java $l > inv-$l.tsv; done
  python3 analyze.py > $R/inventory-summary.md
  python3 counts.py > $R/inventory-counts.txt
  python3 javap_ann.py "$LIBS/f120/*" org.apache.flink.streaming.api.datastream.DataStream \
    org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator org.apache.flink.streaming.api.datastream.KeyedStream \
    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment org.apache.flink.api.common.typeinfo.TypeInformation > $R/f120-annotations.tsv )
for l in f120 f22 f23; do
  extra=$HERE/probes2x/Line.kt; [ $l = f120 ] && extra=$HERE/probes120/Line.kt
  $HERE/build.sh $l $OUT/$l $HERE/probes/*.kt $extra
  for m in probes.ResolutionKt probes.positive.PositiveKt probes.graph.GraphKt probes.runtime.RuntimeKt probes.safety.SafetyNetKt probes.extra.ExtraKt; do
    echo "## $m" ; $HERE/run.sh $l $OUT/$l $m
  done > $R/$l-probes.md
done
for lv in default 2.0; do
  opts=""; [ $lv = 2.0 ] && opts="-language-version 2.0 -api-version 2.0"
  CP=$(ls $LIBS/f23/*.jar | tr '\n' ':')
  for f in $HERE/neg/*.kt; do
    msg=$($KOTLINC $opts -jvm-target 17 -no-reflect -cp "$CP$OUT/f23/classes:$OUT/f23/java" -d $OUT/neg $f 2>&1 | grep "error:" | sed "s|$HERE/||" || true)
    if [ -z "$msg" ]; then echo "$(basename $f): COMPILED"; else echo "$(basename $f): REJECTED"; echo "$msg" | sed 's/^/    /'; fi
  done > $R/negative-compile-f23-lv-$lv.txt
done
