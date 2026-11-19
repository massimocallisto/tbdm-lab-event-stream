"""Publish a small, repeatable CO2 scenario for the ksqlDB windowing lab."""

import argparse
import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


def co2_cycle(minimum, maximum, rise_step, fall_step):
    """Rise to maximum, then fall to minimum as if a window were opened."""
    values = [minimum]
    current = minimum
    while current < maximum:
        current = min(maximum, current + rise_step)
        values.append(current)
    peak_index = len(values) - 1
    while current > minimum:
        current = max(minimum, current - fall_step)
        values.append(current)
    return values, peak_index


def main():
    parser = argparse.ArgumentParser(description="Simulate a room CO2 trend on Kafka")
    parser.add_argument("--bootstrap-server", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="iot.reading.simulated")
    parser.add_argument("--room", default="TeamLab", help="Room with changing CO2")
    parser.add_argument("--reference-room", default="StanzaCulmone", help="Room staying at the baseline")
    parser.add_argument("--min-co2", type=int, default=400)
    parser.add_argument("--max-co2", type=int, default=1200)
    parser.add_argument("--rise-step", type=int, default=100)
    parser.add_argument("--fall-step", type=int, default=200)
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between readings")
    parser.add_argument("--cycles", type=int, default=0, help="Full cycles; 0 means keep running")
    args = parser.parse_args()

    if args.min_co2 < 0 or args.max_co2 <= args.min_co2:
        parser.error("CO2 range must satisfy 0 <= min-co2 < max-co2")
    if args.rise_step <= 0 or args.fall_step <= 0 or args.interval <= 0:
        parser.error("rise-step, fall-step and interval must be positive")
    if args.cycles < 0 or args.room == args.reference_room:
        parser.error("cycles must be nonnegative and room names must differ")

    cycle, peak_index = co2_cycle(args.min_co2, args.max_co2, args.rise_step, args.fall_step)
    producer = Producer({"bootstrap.servers": args.bootstrap_server, "broker.address.family": "v4"})

    try:
        round_number = 0
        while args.cycles == 0 or round_number < args.cycles:
            round_number += 1
            for index, co2 in enumerate(cycle):
                # These are new *simulated* readings, not a replay of historic data.
                now = datetime.now(timezone.utc)
                timestamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
                failures = []

                def delivered(error, message):
                    if error:
                        failures.append(str(error))

                readings = (
                    (args.room, f"{args.room}-01", co2, "ventilation" if index > peak_index else "accumulation"),
                    (args.reference_room, args.reference_room, args.min_co2, "stable"),
                )
                for room, device_id, value, phase in readings:
                    event = {
                        "device_id": device_id,
                        "collected_at": int(now.timestamp()),
                        "collected_at_tz": timestamp,
                        "published_at": timestamp,
                        "sensors": {"co2": value},
                        "phase": phase,
                    }
                    producer.produce(
                        args.topic,
                        key=room.encode("utf-8"),
                        value=json.dumps(event).encode("utf-8"),
                        on_delivery=delivered,
                    )
                    producer.poll(0)

                producer.flush()
                if failures:
                    raise RuntimeError(f"Kafka delivery failed: {failures[0]}")
                print(f"cycle={round_number} room={args.room} co2={co2} phase={readings[0][3]}", flush=True)
                if args.cycles == 0 or round_number < args.cycles or index + 1 < len(cycle):
                    time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
