import json
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer

from configs import kafka_config

MY_NAME = "oleksii_shcherbak"

TOPIC_SOURCE = f"{MY_NAME}_building_sensors"
TOPIC_TEMP_ALERTS = f"{MY_NAME}_temperature_alerts"
TOPIC_HUM_ALERTS = f"{MY_NAME}_humidity_alerts"

consumer = KafkaConsumer(
    TOPIC_SOURCE,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_processor_group",
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Processor listening on '{TOPIC_SOURCE}'. Press Ctrl+C to stop.")

try:
    for message in consumer:
        data = message.value
        print(
            f"[{data['timestamp']}] Sensor {data['sensor_id']} | "
            f"Temp: {data['temperature']}°C | Humidity: {data['humidity']}%"
        )

        if data["temperature"] > 40:
            alert = {
                "sensor_id": data["sensor_id"],
                "temperature": data["temperature"],
                "humidity": data["humidity"],
                "original_timestamp": data["timestamp"],
                "alert_timestamp": datetime.now().isoformat(),
                "alert_message": f"HIGH TEMPERATURE: {data['temperature']}°C exceeds 40°C",
            }
            producer.send(TOPIC_TEMP_ALERTS, value=alert)
            producer.flush()
            print(f"  -> Temperature alert sent for sensor {data['sensor_id']}: {data['temperature']}°C")

        if data["humidity"] > 80:
            alert = {
                "sensor_id": data["sensor_id"],
                "temperature": data["temperature"],
                "humidity": data["humidity"],
                "original_timestamp": data["timestamp"],
                "alert_timestamp": datetime.now().isoformat(),
                "alert_message": f"HIGH HUMIDITY: {data['humidity']}% exceeds 80%",
            }
            producer.send(TOPIC_HUM_ALERTS, value=alert)
            producer.flush()
            print(f"  -> Humidity alert sent for sensor {data['sensor_id']}: {data['humidity']}% (too high)")
        elif data["humidity"] < 20:
            alert = {
                "sensor_id": data["sensor_id"],
                "temperature": data["temperature"],
                "humidity": data["humidity"],
                "original_timestamp": data["timestamp"],
                "alert_timestamp": datetime.now().isoformat(),
                "alert_message": f"LOW HUMIDITY: {data['humidity']}% is below 20%",
            }
            producer.send(TOPIC_HUM_ALERTS, value=alert)
            producer.flush()
            print(f"  -> Humidity alert sent for sensor {data['sensor_id']}: {data['humidity']}% (too low)")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    producer.close()
