import os
import json
import logging
import time
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("service-py")

app = Flask(__name__)

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "host=localhost port=5432 dbname=orders user=postgres password=postgres")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

def connect_postgres():
    for i in range(10):
        try:
            conn = psycopg2.connect(POSTGRES_DSN, cursor_factory=RealDictCursor)
            logger.info("PostgreSQL connected")
            return conn
        except Exception as e:
            logger.warning(f"PostgreSQL not ready, retry {i+1}/10: {e}")
            time.sleep(3)
    raise Exception("Failed to connect to PostgreSQL")

pg_conn = connect_postgres()
with pg_conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            customer TEXT NOT NULL,
            amount NUMERIC NOT NULL,
            status TEXT DEFAULT 'new',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    pg_conn.commit()

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logger.info("Kafka producer connected")
except Exception as e:
    logger.warning(f"Kafka producer failed: {e}")
    kafka_producer = None

@app.route("/healthz", methods=["GET"])
def healthz():
    return "OK", 200

@app.route("/orders", methods=["POST"])
def create_order():
    data = request.get_json()
    customer = data.get("customer")
    amount = data.get("amount")
    if not customer or not amount:
        return jsonify({"error": "customer and amount required"}), 400

    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (customer, amount) VALUES (%s, %s) RETURNING id",
            (customer, amount)
        )
        order_id = cur.fetchone()["id"]
        pg_conn.commit()

    if kafka_producer:
        try:
            kafka_producer.send("order.created", {
                "order_id": order_id,
                "customer": customer,
                "amount": str(amount)
            })
            kafka_producer.flush()
            logger.info(f"Event order.created sent for order {order_id}")
        except Exception as e:
            logger.warning(f"Kafka send failed: {e}")

    logger.info(f"Order {order_id} created")
    return jsonify({"id": order_id, "status": "created"}), 201

@app.route("/orders/<int:order_id>", methods=["GET"])
def get_order(order_id):
    cache_key = f"order:{order_id}"
    cached = redis_client.get(cache_key)
    if cached:
        logger.info(f"Cache hit for order {order_id}")
        return jsonify(json.loads(cached)), 200

    with pg_conn.cursor() as cur:
        cur.execute("SELECT id, customer, amount, status, created_at FROM orders WHERE id = %s", (order_id,))
        order = cur.fetchone()

    if not order:
        return jsonify({"error": "Order not found"}), 404

    order_dict = dict(order)
    order_dict["created_at"] = str(order_dict["created_at"])
    order_dict["amount"] = float(order_dict["amount"])
    redis_client.setex(cache_key, 60, json.dumps(order_dict))
    logger.info(f"Cache miss for order {order_id}, stored in Redis")

    return jsonify(order_dict), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
