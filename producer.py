"""Replay the IoT JSON Lines file to Kafka once every five seconds."""

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer


TOPIC = "iot.reading"


def room_from_device(device_id):
    """AB1-02 -> AB1; StanzaCulmone -> StanzaCulmone."""
    room, separator, sensor = device_id.rpartition("-")
    return room if separator and re.fullmatch(r"\d+", sensor) else device_id


def main():
    parser = argparse.ArgumentParser(description="Simulate IoT events from JSON Lines")
    parser.add_argument("--file", required=True, type=Path, help="Path to an existing JSONL dataset")
    parser.add_argument("--bootstrap-server", default="127.0.0.1:9092")
    parser.add_argument("--interval", type=float, default=5, help="Seconds between complete file replays")
    parser.add_argument("--once", action="store_true", help="Publish the file once and stop")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap_server})
    round_number = 0
    try:
        while True:
            round_number += 1
            failures = []
            count = 0
            print(f"Replay {round_number}: {args.file}", flush=True)

            def delivered(error, message):
                if error:
                    failures.append(str(error))
                else:
                    print(
                        f"sent key={message.key().decode('utf-8')} "
                        f"partition={message.partition()} offset={message.offset()}",
                        flush=True,
                    )

            with args.file.open(encoding="utf-8") as lines:
                for line in lines:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    event["published_at"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    room = room_from_device(event["device_id"])
                    producer.produce(TOPIC, key=room.encode("utf-8"), value=json.dumps(event).encode("utf-8"), on_delivery=delivered)
                    producer.poll(0)
                    count += 1

            producer.flush()
            if failures:
                raise RuntimeError(f"Kafka delivery failed: {failures[0]}")
            print(f"Published {count} events. Press Ctrl+C to stop.\n", flush=True)
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
