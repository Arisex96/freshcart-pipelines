"""Driver: receives the requests whose routing key matches its binding pattern.

Usage:  python driver.py NAME PATTERN
        e.g. python driver.py Ravi "bangalore.koramangala.*"

Broker address comes from the RABBITMQ_URL environment variable
(default: a local broker on localhost with the guest/guest account).
"""
import json
import os
import sys

import pika

EXCHANGE = "rapido-dispatch"

if len(sys.argv) != 3:
    sys.exit("Usage: python driver.py NAME PATTERN")
name, pattern = sys.argv[1], sys.argv[2]
url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

connection = pika.BlockingConnection(pika.URLParameters(url))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic")

# Private queue: the broker picks a random name, and the queue is deleted when this driver disconnects
queue = channel.queue_declare(queue="", exclusive=True).method.queue
channel.queue_bind(exchange=EXCHANGE, queue=queue, routing_key=pattern)
print(f"[{name}] waiting for requests matching '{pattern}'", flush=True)


def on_message(ch, method, properties, body):
    request = json.loads(body)
    print(f"[{name}] got {request['id']}  key={method.routing_key}  {request}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=queue, on_message_callback=on_message)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    connection.close()
