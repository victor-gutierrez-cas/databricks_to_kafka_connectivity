#!/usr/bin/env bash
# Builds the deployable handler JAR against the REAL Databricks shaded Kafka JAR.
# Needs the Databricks shaded Kafka JAR from a cluster on your target runtime (copy it from /databricks/jars to a UC
# volume and download it, or build inside the workspace). Full steps: "Build and deploy" in the top level README.md.
# Usage: ./build.sh /path/to/<databricks-shaded-kafka-clients>.jar
# Only com.danskebank.kafka classes are packaged; the kafkashaded.* classes come from the runtime.
set -euo pipefail
cd "$(dirname "$0")"
KAFKA_JAR="${1:?usage: ./build.sh /path/to/databricks-shaded-kafka.jar}"
rm -rf build/classes && mkdir -p build/classes
javac --release 17 -cp "$KAFKA_JAR" -d build/classes $(find src/main/java -name '*.java')
jar cf build/confluent-entra-handler-instance-profile.jar -C build/classes .
[ -s build/confluent-entra-handler-instance-profile.jar ] || { echo "ERROR: JAR was not written (build on a local disk, not in /Workspace files)" >&2; exit 1; }
if unzip -l build/confluent-entra-handler-instance-profile.jar | grep -q 'kafkashaded/'; then
  echo "ERROR: kafkashaded classes leaked into the JAR" >&2; exit 1
fi
echo "Built build/confluent-entra-handler-instance-profile.jar"
unzip -l build/confluent-entra-handler-instance-profile.jar | grep '\.class' | awk '{print "  " $4}'
