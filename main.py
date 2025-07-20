import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from fetch_data import fetch_data
from storage import upload_to_minio, download_from_minio

logging.basicConfig(level=logging.INFO)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CONSUMER_TOPIC = os.getenv("KAFKA_CONSUMER_TOPIC")
PRODUCER_STG_TOPIC = os.getenv("KAFKA_PRODUCER_STG_SALES_TASKS")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")


async def handle_message(msg):
    task_id = msg["task_id"]
    api_token = msg["wb_token"]
    ts = msg["ts"]
    cards_key = msg['minio_key']

    logging.info(f"Start processing task {task_id}")

    cards = await download_from_minio(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket=MINIO_BUCKET,
        key=cards_key,
    )

    data = await fetch_data(api_token, cards, ts)
    filename = "sales.json"
    prefix = f"{ts}/{task_id}/"
    minio_key = prefix + filename

    await upload_to_minio(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket=MINIO_BUCKET,
        data=data,
        key=minio_key,
    )

    logging.info(f"Task {task_id} completed successfully.")

    return {
        "task_id": task_id,
        "ts": ts,
        "minio_key": minio_key
    }


async def main():
    consumer = AIOKafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="sales-ingestors",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            try:
                next_msg = await handle_message(msg.value)
                encoded_task_id = str(next_msg["task_id"]).encode("utf-8")
                await producer.send(
                    PRODUCER_STG_TOPIC,
                    value=next_msg,
                    key=encoded_task_id,
                )
            except Exception as e:
                # TODO: write task to out of the box table
                logging.error(f"Error processing message: {e}")
    finally:
        logging.info("Stopping consumer.")
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
