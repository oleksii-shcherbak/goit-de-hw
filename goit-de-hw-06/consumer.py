import json

from kafka import KafkaConsumer

from configs import kafka_config

MY_NAME = "oleksii_shcherbak"
TOPIC_ALERTS = f"{MY_NAME}_alerts"

consumer = KafkaConsumer(
    TOPIC_ALERTS,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_alert_reader_group",
)

print(f"Subscribed to '{TOPIC_ALERTS}'. Waiting for alerts... Press Ctrl+C to stop.\n")

try:
    for message in consumer:
        alert = message.value
        window = alert.get("window", {})
        print(
            f"{'=' * 60}\n"
            f"ALERT\n"
            f"  Window:      {window.get('start')}  →  {window.get('end')}\n"
            f"  Avg Temp:    {round(alert.get('t_avg', 0), 4)}°C\n"
            f"  Avg Humidity:{round(alert.get('h_avg', 0), 4)}%\n"
            f"  Code:        {alert.get('code')}\n"
            f"  Message:     {alert.get('message')}\n"
            f"  Timestamp:   {alert.get('timestamp')}\n"
            f"{'=' * 60}"
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Consumer stopped.")
