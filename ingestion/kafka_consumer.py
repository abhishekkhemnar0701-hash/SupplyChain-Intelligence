
import json
import os

from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_SERVER = "localhost:29092"
TOPIC = "supplychain-orders"

OUTPUT_DIR = "data/raw"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "orders.jsonl")


def validate_order(order):
    required_fields = [
        "event_id",
        "order_id",
        "product_id",
        "quantity",
        "total_amount",
        "event_timestamp"
    ]

    for field in required_fields:
        if field not in order:
            raise ValueError(f"Missing field: {field}")

    if not isinstance(order["quantity"], int) or order["quantity"] <= 0:
        raise ValueError("Invalid order quantity")

    if float(order["total_amount"]) <= 0:
        raise ValueError("Invalid total amount")

    return True


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="supplychain-consumer-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    print("Supply Chain Kafka Consumer Started...")

    try:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as file:

            for message in consumer:
                try:
                    order = message.value

                    validate_order(order)

                    file.write(json.dumps(order) + "\n")
                    file.flush()

                    consumer.commit()

                    print(
                        f"Received Order: {order['order_id']} | "
                        f"Amount: {order['total_amount']}"
                    )

                except (ValueError, KeyError, TypeError) as error:
                    print(f"Invalid event: {error}")
                    consumer.commit()

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    except KafkaError as error:
        print(f"Kafka connection error: {error}")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
