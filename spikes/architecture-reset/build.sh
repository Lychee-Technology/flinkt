#!/usr/bin/env bash
# usage: build.sh <f120|f22|f23> <outdir> [extra .kt files...]
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
LIBS=${SPIKE_LIBS:-$HERE/libs}; KOTLINC=${KOTLINC:-$HERE/tools/kotlinc/bin/kotlinc}
L=$1; OUT=$2; shift 2
CP=$(ls $LIBS/$L/*.jar | tr '\n' ':')
case $L in f120) LINE=line120 ;; *) LINE=line2x ;; esac
rm -rf $OUT && mkdir -p $OUT
javac -nowarn -d $OUT/java -cp "$CP" $HERE/java/*.java 2>&1 | grep -v "^Note:" || true
$KOTLINC ${KOTLINC_OPTS:-} -jvm-target 17 -no-reflect -cp "$CP$OUT/java" -d $OUT/classes \
  $(find $HERE/common -name '*.kt') $HERE/$LINE/*.kt "$@" 2>&1 | grep -E "error|warning: language" || true
cp -r $HERE/resources/* $OUT/classes/
