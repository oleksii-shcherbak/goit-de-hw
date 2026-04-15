kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_jaas_config": (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="admin" password="VawEzo1ikLtrA8Ug8THa";'
    ),
}

mysql_config = {
    "host": "217.61.57.46",
    "port": 3306,
    "database": "olympic_dataset",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
}

jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
jdbc_user = mysql_config["user"]
jdbc_password = mysql_config["password"]

output_db = "neo_data"
output_jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{output_db}"
