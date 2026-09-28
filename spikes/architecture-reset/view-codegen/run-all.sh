#!/usr/bin/env bash
# Reads Flink's parameter names from its class files, generates the view forwarders against Flink 2.3.0
# and 1.20.5, runs the probe on each, and checks the generator's refusals, the stability markers it carries
# over, and the guards on the extracted names file. Needs JDK 25 and GRADLE=/path/to/gradle (9.x).
# Writes ../results/view-codegen/.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); R=$HERE/../results/view-codegen; rm -rf $R; mkdir -p $R
G=${GRADLE:-gradle}; DS=org.apache.flink.streaming.api.datastream
cd $HERE
ksp_errors() { grep "^e: \[ksp\]" | sed 's/^e: \[ksp\] [^:]*:[0-9]*: //; s/^e: \[ksp\] //; s/org\.apache\.flink\.[a-z.]*\.//g'; }

# 1. Parameter names from the LocalVariableTable of Flink's class files; fails if any public method has none.
$G --no-daemon -q :extractor:extract_f23 :extractor:extract_f120 2>&1 | grep -v "^w: " > $R/extract.txt

# 2. Generate, compile and run on each line.
for l in f23 f120; do
  $G --no-daemon -q :views-$l:probe 2>&1 | grep -E "^result|^original|^view|^e: " > $R/probe-$l.txt
  mkdir -p $R/generated-$l && cp views-$l/build/generated/ksp/main/kotlin/flinkt/views/*.kt $R/generated-$l/
  cp views-$l/build/generated/ksp/main/resources/flinkt/views/view-codegen-report.txt $R/report-$l.txt
  grep -h "public fun" $R/generated-$l/*.kt | sed 's/ = .*//; s/^ *//' | sort > $R/api-$l.txt
done
diff $R/api-f23.txt $R/api-f120.txt > $R/api-diff.txt

# 3. What the generator refuses, and the @Experimental method it forwards behind an opt-in marker.
for l in f23 f120; do
  for c in DataStream#map DataStream#connect DataStream#broadcast DataStream#getTransformation DataStream#process \
           SingleOutputStreamOperator#getSideOutput DataStream#noSuchMethod SingleOutputStreamOperator#enableAsyncState; do
    msg=$($G --no-daemon -q :views-$l:kspKotlin -PextraForward="$DS.$c" 2>&1 | ksp_errors)
    if [ -z "$msg" ]; then
      msg="GENERATED: $(grep -h -B1 "public fun ${c#*#}(" views-$l/build/generated/ksp/main/kotlin/flinkt/views/*.kt | sed 's/ = .*//; s/^ *//' | tr '\n' ' ')"
    fi
    printf "%s %s\n%s\n\n" "$l" "$c" "$msg"
  done
done > $R/generator-cases.txt

# 4. Kotlin callers see Flink's deprecation and experimental status.
{
  echo "== 1.20: calling a forwarder of a Flink-deprecated method"
  $G --no-daemon :views-f120:compileKotlin -PnegCase=deprecated-call --rerun-tasks 2>&1 | grep "^w: .*UseDeprecated" | sed "s|file://$HERE/||"
  echo "== 2.3: calling a forwarder of a Flink @Experimental method, without and with @OptIn"
  $G --no-daemon :views-f23:compileKotlin -PnegCase=experimental-call -PextraForward=$DS.SingleOutputStreamOperator#enableAsyncState --rerun-tasks 2>&1 \
    | grep "^w: .*UseExperimental" | sed "s|file://$HERE/||"
} > $R/caller-warnings.txt

# 5. Guards on the extracted names file.
{
  echo "== 1.20 adapter built with the 2.3 names file"
  $G --no-daemon -q :views-f120:kspKotlin -PflinkApiFile=../views-f23/flink-api.tsv 2>&1 | ksp_errors | head -2
  echo "== names file without the setParallelism entry"
  grep -v "	setParallelism	" views-f23/flink-api.tsv > $HERE/build-missing.tsv
  $G --no-daemon -q :views-f23:kspKotlin -PflinkApiFile=$HERE/build-missing.tsv 2>&1 | ksp_errors
  rm -f $HERE/build-missing.tsv
  echo "== editing the names file reruns KSP"
  $G --no-daemon -q :views-f23:kspKotlin >/dev/null 2>&1
  cp views-f23/flink-api.tsv $HERE/build-backup.tsv
  python3 - <<'PY'
p = "views-f23/flink-api.tsv"
rows = [r.split("\t") for r in open(p).read().split("\n")]
open(p, "w").write("\n".join("\t".join(r[:3] + ["renamedForTest"] + r[4:]) if len(r) >= 4 and r[1] == "setParallelism" else "\t".join(r) for r in rows))
PY
  $G --no-daemon :views-f23:kspKotlin 2>&1 | grep -E "Task :views-f23:kspKotlin"
  grep -ho "fun setParallelism([a-zA-Z]*" views-f23/build/generated/ksp/main/kotlin/flinkt/views/*.kt
  mv $HERE/build-backup.tsv views-f23/flink-api.tsv
  $G --no-daemon -q :views-f23:kspKotlin >/dev/null 2>&1
  grep -ho "fun setParallelism([a-zA-Z]*" views-f23/build/generated/ksp/main/kotlin/flinkt/views/*.kt
} > $R/names-file-guards.txt
