import json
from kafka import KafkaConsumer
from configs import kafka_config

MY_NAME = "oleksii_shcherbak"
OUTPUT_TOPIC = f"{MY_NAME}_athlete_enriched_agg"

consumer = KafkaConsumer(
    OUTPUT_TOPIC,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print(f"{'sport':<20} {'medal':<10} {'sex':<6} {'country_noc':<12} {'avg_height':<12} {'avg_weight':<12} {'timestamp'}")
print("-" * 100)

count = 0
for msg in consumer:
    r = msg.value
    print(
        f"{str(r.get('sport','')):<20} "
        f"{str(r.get('medal','')):<10} "
        f"{str(r.get('sex','')):<6} "
        f"{str(r.get('country_noc','')):<12} "
        f"{str(r.get('avg_height','')):<12} "
        f"{str(r.get('avg_weight','')):<12} "
        f"{r.get('timestamp','')}"
    )
    count += 1
    if count >= 50:
        break

consumer.close()
print(f"\nShowing {count} records from topic '{OUTPUT_TOPIC}'")
