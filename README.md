# Databricks to Confluent Cloud with Entra ID, without secrets

Two custom Kafka `OAUTHBEARER` login handlers for the Databricks shaded Kafka client. Both use the same chain:
an AWS IAM role gets a signed token from AWS STS (`GetWebIdentityToken`), Entra ID exchanges it through a federated
credential for an access token, and Confluent Cloud validates that token (identity pool, RBAC).

| Folder | JAR | Where the IAM role comes from |
|---|---|---|
| `instance_profile_handler/` | `confluent-entra-handler-instance-profile.jar` | Instance profile on the cluster. Each executor runs the full chain itself; Kafka refreshes tokens automatically |
| `service_credential_handler/` | `confluent-entra-handler-service-credential.jar` | Unity Catalog service credential. The driver mints the token and passes it to the executors; streams restart about every 45 minutes for a fresh token |

Both need dedicated (single user) classic compute: custom Kafka login handlers are blocked on standard access mode
and serverless.

## Build and deploy

The handlers implement the Databricks **shaded** Kafka interfaces (`kafkashaded.org.apache.kafka...`), so they are
compiled against the shaded Kafka JAR that ships inside the Databricks runtime. That JAR is not on Maven: you take it
from a cluster running the same Databricks Runtime version you will run the handler on. It is only needed to compile;
it is never packaged into the handler JAR.

Requirements on the build machine: JDK 17 (`javac` and `jar`), `bash` and `unzip`.

### Option 1: build in your own pipeline or on a laptop (recommended)

1. **Copy the shaded Kafka JAR from the cluster to a UC volume.** In a notebook on a cluster with your target runtime:
   ```
   %sh
   KAFKA_JAR=$(ls /databricks/jars/*kafka-clients_only_shaded-for-kafka*.jar)
   echo "$KAFKA_JAR"
   cp "$KAFKA_JAR" /Volumes/<catalog>/<schema>/<volume>/kafkashaded.jar
   ```
   The file only exists on the cluster's local disk; the volume is how you get it out.
2. **Download it from the volume** to the build machine, either from Catalog Explorer (volume, file, Download) or with
   the Databricks CLI:
   ```bash
   databricks fs cp dbfs:/Volumes/<catalog>/<schema>/<volume>/kafkashaded.jar ./kafkashaded.jar
   ```
3. **Build** from the handler folder:
   ```bash
   cd instance_profile_handler            # or service_credential_handler
   ./build.sh ../kafkashaded.jar          # bash build.sh ... if the execute bit was lost
   ```
   Output: `build/confluent-entra-handler-instance-profile.jar` (or `...-service-credential.jar`).
4. **Upload** the built JAR to a UC volume (Catalog Explorer upload, or
   `databricks fs cp build/<jar> dbfs:/Volumes/<catalog>/<schema>/<volume>/`).
5. **Install** it on a dedicated cluster: Compute, the cluster, Libraries, Install new, Volumes, select the JAR,
   Install, then restart the cluster.

### Option 2: build inside the workspace

Databricks Runtime includes JDK 17 (`javac`, `jar`), so you can also build on the cluster itself, with no download or
upload of the shaded JAR. Put this source in a Git folder or workspace files, then on a dedicated cluster copy it to the
cluster's local disk and build there (building directly in `/Workspace` files is unreliable: in testing the JAR was sometimes not written):
```
%sh
rm -rf /local_disk0/tmp/handlers && cp -r /Workspace/<path>/databricks-confluent-entra-handlers /local_disk0/tmp/handlers
cd /local_disk0/tmp/handlers/instance_profile_handler
bash build.sh "$(ls /databricks/jars/*kafka-clients_only_shaded-for-kafka*.jar)"
cp build/confluent-entra-handler-instance-profile.jar /Volumes/<catalog>/<schema>/<volume>/
```
Then install it as in step 5. Quicker for a first test; option 1 gives you a reviewed, logged and signed build.

### Finding the shaded JAR on other runtimes

The file name pattern above was verified on Databricks Runtime 17.3 LTS. If it matches nothing on your runtime, find
the JAR by its contents:
```
%sh for j in /databricks/jars/*kafka*.jar; do unzip -l "$j" | grep -q kafkashaded/org/apache/kafka/common/security/auth/AuthenticateCallbackHandler.class && echo "$j"; done
```
Rebuild the handler whenever you move to a new Databricks Runtime version.

### Without build.sh

`build.sh` runs these two commands (plus a check that no `kafkashaded` classes end up in the JAR):
```bash
javac --release 17 -cp kafkashaded.jar -d build/classes $(find src/main/java -name '*.java')
jar cf build/<name>.jar -C build/classes .
```
Keep `--release 17`: classes compiled for a newer Java version won't load on the runtime.
