# Kafka to OCI Log Analytics hackathon runbook

## Target and roles

- Production tenancy: **logantest1**.
- Region: **Phoenix / us-phoenix-1**.
- Compartment: **prod under OSLogCollection**; verify the actual path and OCID.
- Source: proposed **Kafka Hackathon Application Logs**, with a matching custom JSON parser.
- Developer owns connector code, chunking/retries and packaging. Shreyam owns the local setup, generator runs,
  LA parser/source verification, event reconciliation and demonstration.
- Namespace, log group OCID and OCI profile have not been filled in or live-validated by this build.

## 1 Start Kafka and the FileStream baseline

Use the official Kafka 4.1.2 Docker broker, retain its container/storage throughout the demo,
and run Connect from the matching Kafka distribution on the host. Kafka must advertise a host-reachable
listener at `localhost:9092`. Create topic `la-hackathon-logs` with one partition and replication factor 1.
Use Kafka console producer/consumer to check transport first.

Copy `config/connect-distributed.properties` to a local file and replace its absolute plugin path.
Install Kafka's connect-file plugin in its own directory beneath that path; it may not be enabled on the default worker classpath.
Keep the FileStream and Oracle Observability plugin directories separate. Start Connect with:

```sh
"$KAFKA_HOME/bin/connect-distributed.sh" config/connect-distributed.local.properties
curl --fail http://localhost:8083/connector-plugins
```

Set a real absolute output file in a local copy of `config/file-sink.json`. Register it:

```sh
curl --fail-with-body -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' --data-binary @config/file-sink.local.json
python3 scripts/generate_logs.py --count 100 --run-id file-baseline \
  | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
      --bootstrap-server localhost:9092 --topic la-hackathon-logs
curl --fail http://localhost:8083/connectors/oci-la-hackathon/status
```

Confirm all 100 unique IDs in the output file. Stop Connect, publish another run, restart and verify backlog delivery.
Keep the Kafka broker and internal Connect topics intact. This stage proves Kafka transport and recovery, not LA parsing.

## 2 Verify the LA source and parser before switching

1. Confirm LA onboarding, actual namespace/log group and profile access to Phoenix.
2. Create or reuse a dedicated log group in the agreed compartment.
3. Create custom JSON parser **Kafka Hackathon JSON** with root `$`.
4. Create a **File** source named **Kafka Hackathon Application Logs** and attach that parser.
5. If the UI requires an entity type, select an appropriate host type. Initially omit entity ID/type from payloads.
6. Configure one fixed log set only if the tenancy requires it.

| JSON path | LA field | Type |
| --- | --- | --- |
| `$.timestamp` | `Time` | Timestamp |
| `$.message` | `Message` | String |
| `$.demo_run_id` | `Hackathon Run ID` | String |
| `$.event_id` | `Hackathon Event ID` | String |
| `$.service` | `Hackathon Service` | String |
| `$.level` | `Hackathon Level` | String |
| `$.latency_ms` | `Hackathon Latency Milliseconds` | Long |

Create custom fields where needed. Verify UTC timestamp parsing, not just ingestion-time fallback.
Use fresh logs from the generator and wrap them with a JSON serializer:

```python
import json
records = [line.rstrip("\n") for line in open("sample.jsonl", encoding="utf-8")]
payload = {"logEvents": [{"logSourceName": "Kafka Hackathon Application Logs",
                          "logPath": "kafka-hackathon.log", "logRecords": records}]}
with open("sample-logevents.json", "w", encoding="utf-8") as out:
    json.dump(payload, out, ensure_ascii=False)
```

Upload that envelope with `UploadLogEventsFile` using `payloadType=JSON`, the verified namespace/log group,
and the agreed OCI profile. `logGroupId` and optional `logSet` belong in request parameters.
Verify all ten sample IDs in Log Explorer, correct source/group, exact message, parsed time and numeric latency.
HTTP 200 alone confirms acceptance; later parsing and indexing must also succeed.

## 3 Install the custom plugin and switch configuration

Build with `mvn clean verify`, extract the plugin ZIP beneath `plugin.path`, and restart Connect.
Check that `/connector-plugins` lists `com.oracle.observability.kafka.connect.OracleObservabilitySinkConnector`.
Copy `config/oci-sink.json` to `config/oci-sink.local.json` and fill in the actual namespace, log group,
absolute OCI config path, profile and source API name. Local JSON configs are git-ignored.

To update an existing FileStream connector, PUT only the `config` object (not the POST wrapper):

```sh
python3 -c 'import json; print(json.dumps(json.load(open("config/oci-sink.local.json"))["config"]))' \
  > /tmp/oci-la-hackathon-config.json
curl --fail-with-body -X PUT http://localhost:8083/connectors/oci-la-hackathon/config \
  -H 'Content-Type: application/json' --data-binary @/tmp/oci-la-hackathon-config.json
curl --fail http://localhost:8083/connectors/oci-la-hackathon/status
```

FileStream already committed its records. Generate a **new run** after the switch; old records will not automatically replay into LA.
Use a separate connector/group for deliberate historical replay. Keep secrets out of the JSON and logs.

## 4 Exercise batching and byte chunking

Count limit demonstration (small logs, requests of at most 100 records; poll boundaries may produce smaller requests):

```sh
python3 scripts/generate_logs.py --count 250 --run-id count-demo \
  | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
      --bootstrap-server localhost:9092 --topic la-hackathon-logs
```

Byte limit demonstration (larger individual logs; byte cap should be reached before count 100):

```sh
python3 scripts/generate_logs.py --count 250 --run-id byte-demo --message-bytes 16000 \
  | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
      --bootstrap-server localhost:9092 --topic la-hackathon-logs
```

Use a fresh run ID each time. Observe accepted records, bytes and latency in Connect logs.
Run small/medium/large datasets of 1, 100 and 1000 records; test quotes, Unicode and embedded multiline strings.
In LA reconcile produced count, unique event IDs, total records, duplicates, missing IDs and indexing delay.
Use manual Log Explorer filters; saved searches and dashboards are deferred.

## 5 Recovery and presentation

Stop Connect while Kafka stays running. Publish 100 new logs and restart with the same connector name and internal state.
Require all unique IDs to appear; replay duplicates can occur. Mock HTTP tests separately prove handling of 429/503.
Check invalid credentials/destination behavior with the mock/local tests before any live negative test.
Do not interpret connector RUNNING or HTTP 200 as proof of correct LA field extraction.

For a five-minute demo: explain the flow, briefly show the FileStream baseline/config switch, send a fresh normal run,
show parsed LA records, send an error spike and filter ERROR, then demonstrate backlog recovery. Allow for measured indexing delay.
Save a successful recording and clearly identify it if used as a fallback.

## Completion gate

- Direct API upload and source/parser preview pass.
- 100 fresh connector-generated IDs are visible and correctly parsed in LA.
- Count and byte chunking produce complete requests within the cap.
- Restart recovery has no missing unique IDs; duplicates are documented.
- Shreyam can repeat the flow from this runbook without changing connector code.
