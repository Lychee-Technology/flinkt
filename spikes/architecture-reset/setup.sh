#!/usr/bin/env bash
# Resolves Flink 1.20.5 / 2.2.1 / 2.3.0 jars into ./libs and downloads kotlinc 2.4.20 into ./tools.
# Needs a JDK (tested with 25) and Gradle 9.x on PATH, or GRADLE=/path/to/gradle.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
(cd "$HERE/resolve" && "${GRADLE:-gradle}" -q copyAll --no-daemon)
mkdir -p "$HERE/tools"
if [ ! -x "$HERE/tools/kotlinc/bin/kotlinc" ]; then
  curl -sSL -o "$HERE/tools/kotlinc.zip" https://github.com/JetBrains/kotlin/releases/download/v2.4.20/kotlin-compiler-2.4.20.zip
  python3 -c "import zipfile,sys; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" "$HERE/tools/kotlinc.zip" "$HERE/tools"
  chmod +x "$HERE/tools/kotlinc/bin/"*
fi
