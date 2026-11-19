# Extension: windowed IoT analytics with ksqlDB

This optional exercise continues the [Kafka IoT lab](README.md). Kafka still transports the events; **ksqlDB is a separate application** that reads a topic, continuously computes per-room results, and writes the results to a new Kafka topic. You will compare a rising CO2 signal with a stable reference room.

## 1. Start the services

Run this command to start the ksqlDB container (it is supposed that you are already running the Kafka container):

```bash
docker compose --profile streaming up -d
docker compose ps
```

The `streaming` profile adds `ksqldb-server` and `ksqldb-cli` to the existing Kafka broker. ksqlDB uses `kafka:19092` inside the Compose network; the Python producer uses `127.0.0.1:9092` on your computer. The ksqlDB HTTP endpoint is available only locally at `127.0.0.1:8088`. Allow some time for the server to start. If the CLI cannot connect yet, inspect `docker compose logs ksqldb-server` and retry. Docker Desktop needs enough memory to run both Kafka and ksqlDB.

Create a **separate** input topic so the simulated readings do not mix with the historical readings in `iot.reading`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic iot.reading.simulated --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic iot.reading.simulated
```
## 2. Register the topic as a stream

Open the ksqlDB interactive console:

```bash
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088
```

The producer will send a room name as a plain Kafka key and a JSON value containing `device_id` and `sensors.co2`. Run this SQL **once** in the ksqlDB console:

```sql
CREATE STREAM IOT_SIMULATED (
  ROOM STRING KEY,
  DEVICE_ID STRING,
  SENSORS STRUCT<CO2 INTEGER>
) WITH (
  KAFKA_TOPIC = 'iot.reading.simulated',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON'
);
```

This command registers an existing Kafka topic. The `ROOM` column comes from its **key**; `DEVICE_ID` and `SENSORS` come from the JSON **value**. The other JSON fields can stay in the messages without being declared here. Plain JSON needs no Schema Registry. `ROWTIME` is initially taken from the Kafka record timestamp, which is close to the producer's `published_at` field in this simulation.

You can inspect individual events with a *push query* (stop it with `Ctrl+C` to return to the console):

```sql
SELECT ROOM, DEVICE_ID, SENSORS->CO2 AS CO2, ROWTIME
FROM IOT_SIMULATED
EMIT CHANGES;
```

## 3. Create a continuous windowed computation

Back at the ksqlDB prompt, create the aggregation **before** launching the producer:

```sql
CREATE TABLE ROOM_CO2_10S
  WITH (KAFKA_TOPIC = 'iot.room.metrics', VALUE_FORMAT = 'JSON', PARTITIONS = 3, REPLICAS = 1)
AS SELECT
  ROOM,
  COUNT(*) AS N_READINGS,
  AVG(SENSORS->CO2) AS AVG_CO2
FROM IOT_SIMULATED
WINDOW TUMBLING (SIZE 10 SECONDS, GRACE PERIOD 4 SECONDS)
GROUP BY ROOM
EMIT CHANGES;
```

This is a **persistent query**, not a one-off database query: it keeps reading new Kafka events. Every ten-second window has a separate count and average for each room. `ROOM_CO2_10S` is a table of changing aggregate results; `iot.room.metrics` is its Kafka output topic. `EMIT CHANGES` can emit several updated values for one window as events arrive. The four-second grace period permits a limited delay for out-of-order events; it does not change the ten-second window size.

In the same console, watch the updates:

```sql
SELECT ROOM, WINDOWSTART, WINDOWEND, N_READINGS, AVG_CO2
FROM ROOM_CO2_10S
EMIT CHANGES;
```

If you prefer an ISODATE display for the windows time:

```sql
SELECT
  ROOM,
  FORMAT_TIMESTAMP(
    FROM_UNIXTIME(WINDOWSTART),
    'yyyy-MM-dd''T''HH:mm:ss.SSSXXX', 'UTC'
  ) AS WINDOW_START_UTC,
  FORMAT_TIMESTAMP(
    FROM_UNIXTIME(WINDOWEND),
    'yyyy-MM-dd''T''HH:mm:ss.SSSXXX', 'UTC'
  ) AS WINDOW_END_UTC,
  N_READINGS,
  AVG_CO2
FROM ROOM_CO2_10S
EMIT CHANGES;
```

Open a **second host terminal** from the repository root and start the simulated producer:

```bash
python producer_co2.py
```

It publishes two **new simulated measurements** every two seconds: `TeamLab-01` grows from 400 to 1200 ppm in steps of 100, then falls to 400 in steps of 200 (a simplified window-opening scenario); `StanzaCulmone` stays at 400 ppm. Both room names are Kafka message keys. The values and the `collected_at`, `collected_at_tz`, and `published_at` timestamps are generated on each tick, so this is not a replay of historic sensor measurements. Stop with `Ctrl+C`.

The cycle and time interval can be changed, for example:

```bash
python producer_co2.py --interval 1 --max-co2 1000 --rise-step 50 --fall-step 150 --cycles 2
```

## 4. Compare with an overlapping window

In the ksqlDB console, run this *temporary* push query while the producer is active:

```sql
SELECT ROOM, WINDOWSTART, WINDOWEND,
       COUNT(*) AS N_READINGS, AVG(SENSORS->CO2) AS AVG_CO2
FROM IOT_SIMULATED
WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 5 SECONDS)
GROUP BY ROOM
EMIT CHANGES;
```

Each hopping window still covers ten seconds, but a new one starts every five seconds. Therefore a single reading can contribute to two windows. Compare the more frequent overlapping results with the ten-second tumbling windows.

### Questions to discuss

1. Why do `TeamLab` and `StanzaCulmone` have different averages if they publish at the same rate?
2. Why can the same `(room, window)` appear more than once in an `EMIT CHANGES` result?
3. Which timestamp would you use to analyze **real** delayed sensor measurements instead of this live simulation? What should happen to a reading that arrives after its window is closed?

## Finish 


```bash
docker compose --profile streaming stop ksqldb-cli ksqldb-server
```

For more about [windowing and event time](https://docs.confluent.io/platform/current/ksqldb/concepts/time-and-windows-in-ksqldb-queries.html), see the ksqlDB documentation.
