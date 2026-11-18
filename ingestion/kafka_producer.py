
import json
import time
import random
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_SERVER = "localhost:29092"
TOPIC = "supplychain-orders"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: str(key).encode("utf-8"),
    acks="all",
    retries=5
)

products = [
    {"product_id": 1, "product_name": "Laptop", "price": 55000},
    {"product_id": 2, "product_name": "Mobile Phone", "price": 25000},
    {"product_id": 3, "product_name": "Office Chair", "price": 7500},
    {"product_id": 4, "product_name": "Desk Table", "price": 12000},
    {"product_id": 5, "product_name": "Headphones", "price": 2000}
]


def generate_order():
    product = random.choice(products)
    quantity = random.randint(1, 5)

    return {
        "event_id": str(uuid.uuid4()),
        "order_id": random.randint(1000, 9999),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": quantity * product["price"],
        "order_status": "NEW",
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }


def main():
    print("Starting Supply Chain Kafka Producer...")

    try:
        while True:
            order = generate_order()

            future = producer.send(
                TOPIC,
                key=order["order_id"],
                value=order
            )

            metadata = future.get(timeout=10)

            print(
                f"Order sent: {order['order_id']} | "
                f"Partition: {metadata.partition} | "
                f"Amount: {order['total_amount']}"
            )

            time.sleep(3)

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    except KafkaError as error:
        print(f"Kafka error: {error}")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
