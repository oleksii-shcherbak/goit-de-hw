import os
import sys

import pyspark
os.environ["SPARK_HOME"] = os.path.join(os.path.dirname(pyspark.__file__))
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, from_json, expr
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, BooleanType,
)

from configs import kafka_config, jdbc_url, jdbc_user, jdbc_password, output_jdbc_url

MY_NAME = "oleksii_shcherbak"
INPUT_TOPIC = f"{MY_NAME}_athlete_event_results"
OUTPUT_TOPIC = f"{MY_NAME}_athlete_enriched_agg"
OUTPUT_TABLE = f"{MY_NAME}_athlete_enriched_agg"

JAR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mysql-connector-j-8.0.32.jar")

_java_opts = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
)

spark = (
    SparkSession.builder
    .appName("OleksiiShcherbak_StreamingPipeline")
    .config("spark.jars", JAR_PATH)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
    .config("spark.driver.extraJavaOptions", _java_opts)
    .config("spark.executor.extraJavaOptions", _java_opts)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Етап 1: зчитати дані фізичних показників атлетів з MySQL
athlete_bio_df = (
    spark.read
    .format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="athlete_bio",
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

# Етап 2: відфільтрувати рядки з порожнім або нечисловим зростом/вагою
athlete_bio_df = (
    athlete_bio_df
    .withColumn("height", expr("try_cast(height as double)"))
    .withColumn("weight", expr("try_cast(weight as double)"))
    .filter(col("height").isNotNull() & col("weight").isNotNull())
)
athlete_bio_df.cache()

# Етап 3: зчитати athlete_event_results з MySQL та записати у вхідний Kafka-топік
athlete_event_results_df = (
    spark.read
    .format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="athlete_event_results",
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

(
    athlete_event_results_df
    .selectExpr("CAST(result_id AS STRING) AS key", "to_json(struct(*)) AS value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
    .option("topic", INPUT_TOPIC)
    .option("checkpointLocation", f"/tmp/{MY_NAME}_checkpoint_write")
    .save()
)

# Зчитати дані з Kafka-топіку як стрім, десеріалізувати JSON у DataFrame
event_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", BooleanType(), True),
])

kafka_streaming_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "300")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), event_schema).alias("data"))
    .select("data.*")
)

# Етап 4: об'єднати стрім подій з біологічними даними за athlete_id
joined_df = kafka_streaming_df.drop("country_noc").join(athlete_bio_df, "athlete_id")

# Етап 5: середній зріст і вага за комбінацією sport / medal / sex / country_noc
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)


def foreach_batch_function(batch_df, batch_id):
    batch_df.show(10, truncate=False)

    # Етап 6.а): запис у вихідний Kafka-топік
    (
        batch_df
        .selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
        .option("topic", OUTPUT_TOPIC)
        .save()
    )

    # Етап 6.б): запис у базу даних
    (
        batch_df.write
        .format("jdbc")
        .option("url", output_jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", OUTPUT_TABLE)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .mode("append")
        .save()
    )


# Етап 6: запустити writeStream з forEachBatch
(
    aggregated_df
    .writeStream
    .foreachBatch(foreach_batch_function)
    .outputMode("update")
    .option("checkpointLocation", f"/tmp/{MY_NAME}_checkpoint_stream")
    .start()
    .awaitTermination()
)
