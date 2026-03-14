from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import kafka_config

MY_NAME = "oleksii_shcherbak"

TOPICS = [
    f"{MY_NAME}_building_sensors",
    f"{MY_NAME}_temperature_alerts",
    f"{MY_NAME}_humidity_alerts",
]

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

for topic_name in TOPICS:
    try:
        admin_client.create_topics(
            new_topics=[NewTopic(name=topic_name, num_partitions=2, replication_factor=1)],
            validate_only=False,
        )
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")

print([topic for topic in admin_client.list_topics() if MY_NAME in topic])

admin_client.close()
