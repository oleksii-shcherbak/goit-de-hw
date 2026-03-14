import json

from kafka import KafkaConsumer

from configs import kafka_config

MY_NAME = "oleksii_shcherbak"

TOPIC_TEMP_ALERTS = f"{MY_NAME}_temperature_alerts"
TOPIC_HUM_ALERTS = f"{MY_NAME}_humidity_alerts"

consumer = KafkaConsumer(
    TOPIC_TEMP_ALERTS,
    TOPIC_HUM_ALERTS,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_alert_consumer_group",
)

print(f"Subscribed to '{TOPIC_TEMP_ALERTS}' and '{TOPIC_HUM_ALERTS}'. Press Ctrl+C to stop.")

try:
    for message in consumer:
        alert = message.value
        print(
            f"\n{'=' * 55}\n"
            f"ALERT from topic: {message.topic}\n"
            f"  Sensor ID:   {alert['sensor_id']}\n"
            f"  Temperature: {alert['temperature']}°C\n"
            f"  Humidity:    {alert['humidity']}%\n"
            f"  Timestamp:   {alert['original_timestamp']}\n"
            f"  Message:     {alert['alert_message']}\n"
            f"{'=' * 55}"
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
