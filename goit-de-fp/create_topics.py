from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

MY_NAME = "oleksii_shcherbak"
TOPIC_NAMES = [
    f"{MY_NAME}_athlete_event_results",
    f"{MY_NAME}_athlete_enriched_agg",
]

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

new_topics = [
    NewTopic(name=name, num_partitions=2, replication_factor=1)
    for name in TOPIC_NAMES
]

try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"Created: {TOPIC_NAMES}")
except Exception as e:
    print(f"Note: {e}")

for topic in admin_client.list_topics():
    if MY_NAME in topic:
        print(f"  {topic}")

admin_client.close()
