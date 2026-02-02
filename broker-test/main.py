import os
from dotenv import load_dotenv, find_dotenv
import time
import uuid
from kafka import KafkaProducer, KafkaConsumer
import pika
import json


# Загружаем .env
dotenv_path = find_dotenv()
if not dotenv_path:
    raise RuntimeError(
        "❌ Файл .env не найден! Создайте .env с BIND_IP, RABBITMQ_DEFAULT_USER, RABBITMQ_DEFAULT_PASS"
    )

load_dotenv(dotenv_path)


# Безопасное чтение env (str | None -> str)
def get_env_str(key: str) -> str:
    value = os.getenv(key)
    if value is None:
        raise RuntimeError(f"❌ '{key}' отсутствует в .env")
    return value.strip()


BIND_IP = get_env_str("BIND_IP")
RABBIT_USER = get_env_str("RABBITMQ_DEFAULT_USER")
RABBIT_PASS = get_env_str("RABBITMQ_DEFAULT_PASS")


print(f"✅ Конфиг:")
print(f"   IP:     {BIND_IP}")
print(f"   User:   {RABBIT_USER}")
print()


# Kafka
KAFKA_BOOTSTRAP = f"{BIND_IP}:9092"
KAFKA_TOPIC = "test-topic"


# RabbitMQ
RABBIT_HOST = BIND_IP
RABBIT_PORT = 5672
RABBIT_QUEUE = "test-queue"


def test_kafka():
    print("🟡 Kafka")
    print("━" * 30)

    # Producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print("📤 →")
    for i in range(5):
        msg = {"msg": f"Kafka #{i+1}"}
        producer.send(KAFKA_TOPIC, value=msg)
        print(f"   {msg['msg']}")
    producer.flush()
    producer.close()

    print("⏳ ..")
    time.sleep(2)

    # Consumer
    group_id = f"kafka-test-{uuid.uuid4().hex[:6]}"
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        max_poll_records=10,
    )

    received = 0
    timeout = 10
    start_time = time.time()

    print("📥 ←")
    try:
        for msg in consumer:
            print(f"   {msg.value['msg']}")
            received += 1
            start_time = time.time()
            if received >= 5:
                break
            if time.time() - start_time > timeout:
                print("   ⏰ timeout")
                break
    finally:
        consumer.close()

    print(f"✅ {received}/5")
    print()


def test_rabbitmq():
    print("🟡 RabbitMQ")
    print("━" * 30)

    try:
        credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
        params = pika.ConnectionParameters(
            host=RABBIT_HOST, port=RABBIT_PORT, credentials=credentials
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=RABBIT_QUEUE, durable=True)

        # Producer
        print("📤 →")
        for i in range(5):
            msg = f"RMQ #{i+1}"
            channel.basic_publish(exchange="", routing_key=RABBIT_QUEUE, body=msg)
            print(f"   {msg}")

        # Consumer
        print("📥 ←")
        received = 0
        while received < 5:
            _, _, body = channel.basic_get(RABBIT_QUEUE, auto_ack=True)
            if body is None:
                print("   ⏰ empty")
                break
            print(f"   {body.decode()}")
            received += 1
            time.sleep(0.1)

        channel.queue_purge(RABBIT_QUEUE)
        connection.close()
        print(f"✅ {received}/5")
        print()

    except Exception as e:
        print(f"❌ {e}")


if __name__ == "__main__":
    test_kafka()
    test_rabbitmq()
    print("🎉 Готово!")
