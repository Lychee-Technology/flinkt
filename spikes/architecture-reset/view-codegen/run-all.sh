#!/usr/bin/env bash
# Generates the view forwarders against Flink 2.3.0 and 1.20.5, runs the probe on each, and runs the
# generator's negative cases. Needs JDK 25 and GRADLE=/path/to/gradle (9.x). Writes ../results/view-codegen/.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); R=$HERE/../results/view-codegen; mkdir -p $R
G=${GRADLE:-gradle}; DS=org.apache.flink.streaming.api.datastream
cd $HERE
for l in f23 f120; do
  $G --no-daemon -q :views-$l:probe 2>&1 | grep -E "^result|^original|^view|^e: " > $R/probe-$l.txt
  cp views-$l/build/generated/ksp/main/kotlin/flinkt/views/*.kt $R/ 2>/dev/null
  mkdir -p $R/generated-$l && mv $R/Flinkt*Forwarders.kt $R/generated-$l/
  cp views-$l/build/generated/ksp/main/resources/flinkt/views/view-codegen-report.txt $R/report-$l.txt
  grep -h "public fun" $R/generated-$l/*.kt | sed 's/ = .*//; s/^ *//' | sort > $R/api-$l.txt
done
diff $R/api-f23.txt $R/api-f120.txt > $R/api-diff.txt && echo "identical" >> $R/api-diff.txt
for l in f23 f120; do
  for c in DataStream#map DataStream#connect DataStream#broadcast DataStream#getTransformation DataStream#process \
           SingleOutputStreamOperator#enableAsyncState SingleOutputStreamOperator#getSideOutput DataStream#noSuchMethod; do
    msg=$($G --no-daemon -q :views-$l:kspKotlin -PextraForward="$DS.$c" 2>&1 | grep "^e: \[ksp\]" | sed 's/^e: \[ksp\] [^:]*:[0-9]*: //; s/org\.apache\.flink\.[a-z.]*\.//g')
    printf "%s %s\n%s\n\n" "$l" "$c" "${msg:-GENERATED (no error)}"
  done
done > $R/negative-cases.txt
