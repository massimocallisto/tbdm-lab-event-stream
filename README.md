# IoT events with Apache Kafka — TBDM Module 5

This lab turns the IoT JSON Lines dataset from the earlier NoSQL lab into a small, repeatable event stream. You will publish events to Kafka, inspect their keys, partitions and offsets, and run two independent consumer groups.

## What you need

- **Recommended:** Docker Desktop (Windows or macOS), or Docker Engine plus the `docker compose` plugin (Linux).
- Python 3.10 or newer, with `pip`. Python programs run **on your computer**, not in the Kafka container.
- The IoT JSONL dataset used in the previous lab. It is **not included** here: copy it locally or provide its existing path when you start the producer. Each line must be a JSON object with `device_id` and `sensors`, for example `{"device_id":"AB1-02","sensors":{"co2":473}}`.

This repository has no Python dependency on Java. Docker runs the Kafka broker and provides its command-line utilities. A native Kafka setup is described [below](#alternative-run-kafka-without-docker).

## 1. Start Kafka (Docker)

From the directory containing `compose.yaml`:

```bash
docker compose up -d
docker compose logs -f
```

After log inspection you can close the command (ctrl+c).

Allow the broker a short time to start. The container uses the official `apache/kafka:4.1.2` image in single-node KRaft mode; its endpoint for the Python programs is `localhost:9092`. Docker is used only for the broker. The one-broker setup has replication factor 1 and is for learning.

Create a topic with **three partitions** (run once; the command is safe to repeat):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic iot.reading --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic iot.reading
```

If the broker is still starting, wait a few seconds and try the topic command again. Use a terminal such as macOS Terminal, Windows PowerShell or a Linux terminal. The same `docker compose` commands work in all three.

## 2. Install the Python client

**macOS / Linux:**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

**Windows PowerShell:**

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

In the commands below, `python` means the Python interpreter inside `.venv`. On Windows PowerShell you can use `.\.venv\Scripts\python.exe` instead of `python` without activating anything.

## 3. Publish events from the JSONL file

Copy the previous lab's JSONL file to `data/device_data-example.jsonl`, or change the path below to its real location. The `data/` directory and the dataset are not distributed with this repository.

```bash
python producer.py --file data/device_data-example.jsonl
```

The producer reads the **whole file**, publishes each row, waits **5 seconds**, then reads it again. It keeps going until `Ctrl+C`. To do just one pass, add `--once`. A different interval can be set with `--interval 2`.

| Kafka field | Value in this lab |
| --- | --- |
| Topic | `iot.reading` |
| Key | Room extracted from `device_id`: `AB1-02` → `AB1`, `TeamLab-01` → `TeamLab`, `StanzaCulmone` → `StanzaCulmone` |
| Value | The original JSON object plus `published_at`, an ISO 8601 UTC time set immediately before sending |

The original `collected_at` and `collected_at_tz` remain unchanged: they describe the historical sensor reading. Replaying the file creates *new publications of the same sample measurements*, not new measurements. The producer prints the **actual** Kafka partition and offset after delivery. Different rooms may share a partition; with the partition count unchanged, events with the same room key go to the same partition under the default partitioner.

## 4. First read: the Kafka console

After the producer has published at least one round, open a **second terminal** and run:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot.reading --from-beginning --property print.key=true --property print.partition=true --property print.offset=true
```

You will see existing events first and then new ones as they arrive. Look for repeated room keys and compare their partition and offset values. Offsets are assigned **within a partition**; offset 4 in partition 0 is not the same position as offset 4 in partition 1. Stop with `Ctrl+C`.

`--from-beginning` applies when the console consumer has no previously committed position. If you supply a group name that has already committed offsets, its saved position normally takes precedence. For this first observation, you do not need to supply a group name.

## 5. Inspect only one room (Python)

Open a new terminal with the Python environment from step 2:

```bash
python consumer.py --group room-view --room TeamLab
```

The output includes `skipped` lines for other rooms. This is intentional: **Kafka sends records from the assigned partitions, and our Python program filters by key after reading them**. Filtering this way does not make the broker transmit only TeamLab events. Use a new group name such as `room-view-2` if you want to replay all retained messages from the start.

## 6. Two independent applications

Start the following consumers in **two separate terminals**. They read the same topic independently because their group names differ:

```bash
python consumer.py --group alerts --mode alerts --co2-threshold 500
```

```bash
python consumer.py --group storage --mode storage
```

The `alerts` consumer prints a demonstration label when `sensors.co2` is above 500; this number is an **exercise threshold**, not a safety recommendation. The `storage` consumer appends each received JSON value to `output/readings-<process-id>.jsonl` on your computer. We use a separate file for each process so two consumers do not append to the same file. Replayed sample readings, or recovery after a failure, can create repeated rows in these files.

Each program commits its group's **next offset** after handling a message. Kafka stores committed positions separately for `alerts` and `storage`. A group that has already committed offsets resumes from them; the `earliest` setting in the Python consumer is only used for a *new* group (or when a saved offset is no longer valid).

## 7. Add a second consumer to a group

While the first `alerts` program is running, open another terminal and run the **same command**:

```bash
python consumer.py --group alerts --mode alerts --co2-threshold 500
```

The two members of `alerts` divide the three partitions between them. Each partition is handled by one member of that group at a time; the `storage` group continues independently. Stop one `alerts` consumer with `Ctrl+C` and observe that the surviving member takes over its partitions. Startup and reassignment may take a short time. Since all TeamLab events have the same key, they stay on the same partition and go to one `alerts` member at a time. A group cannot actively divide three partitions among more than three members.

If you want to see the committed offsets for a group, run:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group alerts
```

This is an **optional observation**. The core lab does not use manual partition assignment, `seek()` or offset resets.

## Stop and restart

Stop the producer and consumers with `Ctrl+C`, then stop Kafka:

```bash
docker compose down
```

The example keeps broker data inside the disposable container. After `docker compose down`, recreating the container gives you a fresh lab: create the topic again. Output files made by the storage program stay in your local `output/` directory. Port `9092` must be free before starting Kafka.

## Alternative: run Kafka without Docker

If you already use a local Kafka installation on **macOS or Linux**, you can run Kafka 4.1.2 natively using the [official Apache Kafka quickstart](https://kafka.apache.org/41/getting-started/quickstart/). This requires **Java 17+**. In the extracted Kafka directory, initialize the standalone KRaft broker **once**, then start it:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

In another terminal, from the same Kafka directory, create the lab topic:

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic iot.reading --partitions 3 --replication-factor 1
```

For the console and group-inspection commands above, replace `docker compose exec kafka /opt/kafka/bin/` with `bin/` and run them from the Kafka directory. Run `producer.py` and `consumer.py` from **this repository**, with their Python environment, as before. Do not format an already initialized Kafka log directory again. On Windows, the Docker path above is the simplest; the native Linux instructions also work within WSL if you have a suitable Java installation there.

## If something does not work

- **Connection refused:** Check `docker compose ps`; the broker may still be starting. Also check that no other program is already using port `9092`.
- **Connection refused (2):** if there is some error about an unresolved host, try to put kafka to your file hosts (e.g., /etc/hosts under linux/windows)
- Check `docker compose ps`; the broker may still be starting. Also check that no other program is already using port `9092`.
- **No output in the console:** Confirm that the producer printed `sent ...`, that the topic is `iot.reading`, and that you used `--from-beginning` for the initial observation.
- **No output from an existing Python group:** The group may already be at the end of the topic; leave the producer running or choose a new group name.
- **No JSONL file:** Specify the real path after `--file`. This ZIP does not include the earlier dataset.

## References

- [Apache Kafka 4.1 Docker image](https://kafka.apache.org/41/getting-started/docker/)
- [Apache Kafka 4.1 quickstart](https://kafka.apache.org/41/getting-started/quickstart/)
- [Confluent Python client API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
