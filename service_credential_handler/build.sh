#!/usr/bin/env bash
# Builds the token relay handler JAR against the REAL Databricks shaded Kafka JAR.
# Needs the Databricks shaded Kafka JAR from a cluster on your target runtime (copy it from /databricks/jars to a UC
# volume and download it, or build inside the workspace). Full steps: "Build and deploy" in the top level README.md.
# Usage: ./build.sh /path/to/<databricks-shaded-kafka-clients>.jar
# Separate artifact from ../instance_profile_handler (different package and JAR name).
set -euo pipefail
cd "$(dirname "$0")"
KAFKA_JAR="${1:?usage: ./build.sh /path/to/databricks-shaded-kafka.jar}"
OUT=build/confluent-entra-handler-service-credential.jar
rm -rf build/classes && mkdir -p build/classes
javac --release 17 -cp "$KAFKA_JAR" -d build/classes $(find src/main/java -name '*.java')
jar cf "$OUT" -C build/classes .
[ -s "$OUT" ] || { echo "ERROR: JAR was not written (build on a local disk, not in /Workspace files)" >&2; exit 1; }
if unzip -l "$OUT" | grep -q 'kafkashaded/'; then
  echo "ERROR: kafkashaded classes leaked into the JAR" >&2; exit 1
fi
echo "Built $OUT"
unzip -l "$OUT" | grep '\.class' | awk '{print "  " $4}'
