"""One small consumer for viewing, alerting, and local storage exercises."""

import argparse
import json
import os
from pathlib import Path

from confluent_kafka import Consumer


TOPIC = "iot.reading"


def main():
    """
    Main function that runs a Kafka consumer for IoT sensor readings.
    
    This consumer subscribes to the "iot.reading" Kafka topic and processes messages
    in one of three modes:
    - view: Display all IoT events (or filtered by room)
    - alerts: Monitor CO2 levels and alert when threshold is exceeded
    - storage: Persist all events to a local JSONL file
    
    The consumer uses consumer groups to distribute partitions among multiple instances
    with the same group ID, enabling horizontal scaling.
    
    Command-line Arguments:
        --group: Consumer group ID (required). Consumers with the same group ID
                 will divide the partitions among themselves.
        --mode: Processing mode - one of 'view', 'alerts', or 'storage' (default: 'view')
        --room: Optional filter to process events only from a specific room/device
        --co2-threshold: CO2 level threshold (ppm) for alert mode (default: 500)
        --bootstrap-server: Kafka broker address (default: 'localhost:9092')
    
    Message Format:
        Expected message structure in Kafka:
        - Key: Room/device identifier (string)
        - Value: JSON object with keys: 'device_id', 'published_at', 'sensors' (containing 'co2', etc.)
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Read IoT events from Kafka")
    parser.add_argument(
        "--group",
        required=True,
        help="Consumers sharing this name divide the partitions"
    )
    parser.add_argument(
        "--mode",
        choices=("view", "alerts", "storage"),
        default="view",
        help="Processing mode: view (display), alerts (CO2 monitoring), or storage (save to file)"
    )
    parser.add_argument(
        "--room",
        help="Show only this room (filter happens in this program)"
    )
    parser.add_argument(
        "--co2-threshold",
        type=float,
        default=500,
        help="Demo alert threshold for CO2 levels (ppm)"
    )
    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9092",
        help="Kafka broker address"
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        default=False,
        help="Define initial offset position"
    )
    args = parser.parse_args()

    # Initialize Kafka consumer with configuration
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_server,
        "group.id": args.group,
        "auto.offset.reset": "earliest",  # Start from the beginning if no saved offset exists
        "enable.auto.commit": False,  # Manually commit to ensure message processing
    })

    from confluent_kafka import OFFSET_BEGINNING

    from confluent_kafka import OFFSET_BEGINNING  # aggiungi agli import

    def read_from_beginning(consumer, partitions):
        print("Assigned:", [(p.topic, p.partition) for p in partitions], flush=True)
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
        consumer.assign(partitions)



    if args.from_beginning:
        print("use from_beginning")
        consumer.subscribe([TOPIC], on_assign=read_from_beginning)
    else:
        consumer.subscribe([TOPIC])

    #consumer.subscribe([TOPIC])  # Subscribe to topic; partitions assigned by Kafka group coordinator

    # Prepare storage file if in storage mode
    storage_file = None
    if args.mode == "storage":
        Path("output").mkdir(exist_ok=True)
        storage_path = Path("output") / f"readings-{os.getpid()}.jsonl"
        storage_file = storage_path.open("a", encoding="utf-8")
        print(f"Writing to {storage_path}", flush=True)

    # Print startup information
    print(
        f"group={args.group} mode={args.mode} room={args.room or 'all'} (Ctrl+C to stop)",
        flush=True
    )
    
    try:
        # Main message processing loop
        while True:
            # Poll for new messages (1 second timeout)
            message = consumer.poll(1.0)
            if message is None:
                # No message received within timeout; continue polling
                continue
            if message.error():
                # Handle Kafka errors
                raise RuntimeError(message.error())

            # Extract message metadata and content
            key = message.key().decode("utf-8")  # Room/device identifier
            event = json.loads(message.value())  # Parse JSON payload
            location = f"partition={message.partition()} offset={message.offset()} key={key}"

            # Apply room filter if specified
            if args.room and key != args.room:
                print(f"skipped {location}", flush=True)
            # Store event to file
            elif args.mode == "storage":
                storage_file.write(json.dumps(event) + "\n")
                storage_file.flush()
                print(f"stored  {location}", flush=True)
            # Check CO2 levels and alert if needed
            elif args.mode == "alerts":
                co2 = event["sensors"]["co2"]
                label = "ALERT" if co2 > args.co2_threshold else "normal"
                print(f"{label:6} {location} co2={co2}", flush=True)
            # Display event details in view mode (default)
            else:
                print(
                    f"view    {location} device={event['device_id']} published_at={event['published_at']}",
                    flush=True
                )

            # Commit the next offset after successful message processing
            # asynchronous=False ensures the offset is persisted before continuing
            consumer.commit(message=message, asynchronous=False)
            
    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C
        print("Consumer stopped.")
    finally:
        # Cleanup: close file handles and consumer connection
        if storage_file:
            storage_file.close()
        consumer.close()


if __name__ == "__main__":
    main()