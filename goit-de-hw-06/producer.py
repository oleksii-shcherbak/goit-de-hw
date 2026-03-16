import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

from configs import kafka_config

MY_NAME = "oleksii_shcherbak"
TOPIC = f"{MY_NAME}_building_sensors"

sensor_id = random.randint(1000, 9999)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Sensor {sensor_id} started. Sending to '{TOPIC}'. Press Ctrl+C to stop.")

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().isoformat(),
            "temperature": round(random.uniform(25, 65), 2),
            "humidity": round(random.uniform(15, 85), 2),
        }
        producer.send(TOPIC, value=data)
        producer.flush()
        print(
            f"[{data['timestamp']}] Sensor {sensor_id} | "
            f"Temp: {data['temperature']}°C | Humidity: {data['humidity']}%"
        )
        time.sleep(2)
except KeyboardInterrupt:
    pass
finally:
    producer.close()
    print(f"Sensor {sensor_id} stopped.")
