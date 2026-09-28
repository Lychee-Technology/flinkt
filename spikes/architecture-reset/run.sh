#!/usr/bin/env bash
# usage: run.sh <f120|f22|f23> <outdir> <main class>
HERE=$(cd "$(dirname "$0")" && pwd)
LIBS=${SPIKE_LIBS:-$HERE/libs}; KOTLINC=${KOTLINC:-$HERE/tools/kotlinc/bin/kotlinc}
L=$1; OUT=$2; MAIN=$3
CP=$(ls $LIBS/$L/*.jar | tr '\n' ':')
# kotlin-stdlib only: kotlin-reflect is deliberately absent from the runtime classpath.
java --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
  -cp "$OUT/classes:$OUT/java:$CP$(dirname $KOTLINC)/../lib/kotlin-stdlib.jar" $MAIN 2>&1 \
  | grep -v "WARN\|SLF4J\|^WARNING\|MiniCluster is not yet running\|^\s*at " | sed 's/ (Kotlin reflection is not available)//g'
