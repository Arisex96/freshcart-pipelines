"""Dispatcher: publishes a random ride/food request every 2 seconds to the topic exchange.

Usage:  python dispatcher.py            (runs until Ctrl+C)
        python dispatcher.py 60         (stops after 60 messages)

Broker address comes from the RABBITMQ_URL environment variable
(default: a local broker on localhost with the guest/guest account).
"""
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

import pika

EXCHANGE = "rapido-dispatch"
ZONES = {
    "bangalore": ["koramangala", "indiranagar", "whitefield"],
    "chennai": ["tnagar", "whitefield"],
}
TYPES = ["ride", "food"]

url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
max_messages = int(sys.argv[1]) if len(sys.argv) > 1 else None

connection = pika.BlockingConnection(pika.URLParameters(url))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic")

count = 0
try:
    while max_messages is None or count < max_messages:
        count += 1
        city = random.choice(list(ZONES))
        zone = random.choice(ZONES[city])
        order_type = random.choice(TYPES)
        routing_key = f"{city}.{zone}.{order_type}"

        message = {
            "id": f"REQ-{count:04d}",
            "city": city,
            "zone": zone,
            "type": order_type,
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        channel.basic_publish(exchange=EXCHANGE, routing_key=routing_key, body=json.dumps(message))
        print(f"sent {message['id']}  key={routing_key}  {json.dumps(message)}", flush=True)
        time.sleep(2)
except KeyboardInterrupt:
    pass
finally:
    connection.close()
