import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from configs import kafka_config


MY_NAME = "oleksii_shcherbak"
INPUT_TOPIC = f"{MY_NAME}_building_sensors"
OUTPUT_TOPIC = f"{MY_NAME}_alerts"

KAFKA_BOOTSTRAP = kafka_config["bootstrap_servers"][0]
KAFKA_USERNAME = kafka_config["username"]
KAFKA_PASSWORD = kafka_config["password"]

ALERTS_CSV_PATH = os.path.join(os.path.dirname(__file__), "alerts_conditions.csv")

WINDOW_DURATION = "1 minute"
SLIDING_INTERVAL = "30 seconds"
WATERMARK_DURATION = "10 seconds"

KAFKA_JAAS = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_USERNAME}" '
    f'password="{KAFKA_PASSWORD}";'
)

SENSOR_SCHEMA = StructType(
    [
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
    ]
)


def build_spark_session() -> SparkSession:
    """Create and return a configured SparkSession.

    Returns:
        A SparkSession with the application name set.
    """
    return (
        SparkSession.builder.appName("IoT_Sensor_Alert_Processor")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_alerts_conditions(spark: SparkSession):
    """Load alert condition thresholds from the CSV file into a Spark DataFrame.

    The CSV uses -999 to indicate that a particular boundary is not active
    for a given alert rule. These are retained as-is and handled during
    the filter step.

    Args:
        spark: An active SparkSession.

    Returns:
        A Spark DataFrame with columns:
        id, temperature_min, temperature_max,
        humidity_min, humidity_max, code, message.
    """
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(ALERTS_CSV_PATH)
    )


def read_kafka_stream(spark: SparkSession):
    """Connect to the Kafka input topic and return a raw streaming DataFrame.

    Args:
        spark: An active SparkSession.

    Returns:
        A streaming DataFrame with Kafka envelope columns (key, value, etc.).
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", KAFKA_JAAS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_sensor_stream(raw_stream):
    """Deserialise the Kafka value bytes and cast columns to their proper types.

    Each Kafka message value is a UTF-8 encoded JSON string. This function
    converts it into a structured streaming DataFrame with a proper event-time
    column called `event_time`.

    Args:
        raw_stream: The raw Kafka streaming DataFrame.

    Returns:
        A structured streaming DataFrame with typed sensor columns.
    """
    return (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_string")
        .select(F.from_json(F.col("json_string"), SENSOR_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp")),
        )
    )


def compute_window_averages(parsed_stream):
    """Apply a sliding window and compute average temperature and humidity.

    A watermark of 10 seconds is declared first to tell Spark how late data
    can arrive before being discarded. Then a sliding window of 1 minute
    length, recalculated every 30 seconds, is applied. Within each window
    bucket, the average temperature and average humidity are computed.

    Args:
        parsed_stream: The structured streaming DataFrame from parse_sensor_stream.

    Returns:
        A streaming DataFrame with columns:
        window (start, end), t_avg, h_avg.
    """
    return (
        parsed_stream.withWatermark("event_time", WATERMARK_DURATION)
        .groupBy(F.window(F.col("event_time"), WINDOW_DURATION, SLIDING_INTERVAL))
        .agg(
            F.avg("temperature").alias("t_avg"),
            F.avg("humidity").alias("h_avg"),
        )
    )


def apply_alert_conditions(windowed_stream, alerts_df):
    """Cross join window averages with alert conditions and filter for violations.

    A cross join combines every window row with every alert rule row,
    producing all possible (window, rule) pairs. The subsequent filter
    keeps only the pairs where the averaged values actually violate the rule.

    The sentinel value -999 means the boundary is inactive. The condition
    logic checks:
        - temperature_min: skip if -999, else flag if t_avg < temperature_min
        - temperature_max: skip if -999, else flag if t_avg > temperature_max
        - humidity_min:    skip if -999, else flag if h_avg < humidity_min
        - humidity_max:    skip if -999, else flag if h_avg > humidity_max

    Args:
        windowed_stream: The streaming DataFrame from compute_window_averages.
        alerts_df: The static Spark DataFrame of alert condition rows.

    Returns:
        A streaming DataFrame of triggered alert rows.
    """
    crossed = windowed_stream.crossJoin(alerts_df)

    temperature_violated = (
        (F.col("temperature_min") != -999) & (F.col("t_avg") < F.col("temperature_min"))
    ) | (
        (F.col("temperature_max") != -999) & (F.col("t_avg") > F.col("temperature_max"))
    )

    humidity_violated = (
        (F.col("humidity_min") != -999) & (F.col("h_avg") < F.col("humidity_min"))
    ) | (
        (F.col("humidity_max") != -999) & (F.col("h_avg") > F.col("humidity_max"))
    )

    return crossed.filter(temperature_violated | humidity_violated)


def format_for_kafka(alerts_stream):
    """Serialise triggered alert rows as JSON strings for writing to Kafka.

    Each alert message follows the schema shown in the homework specification:
    window start/end, average temperature, average humidity, alert code,
    alert message, and the processing timestamp.

    Args:
        alerts_stream: The streaming DataFrame from apply_alert_conditions.

    Returns:
        A streaming DataFrame with a single `value` column (JSON string).
    """
    return alerts_stream.select(
        F.to_json(
            F.struct(
                F.struct(
                    F.col("window.start").cast(StringType()).alias("start"),
                    F.col("window.end").cast(StringType()).alias("end"),
                ).alias("window"),
                F.col("t_avg"),
                F.col("h_avg"),
                F.col("code").cast(StringType()).alias("code"),
                F.col("message"),
                F.current_timestamp().cast(StringType()).alias("timestamp"),
            )
        ).alias("value")
    )


def write_alerts_to_kafka(formatted_stream):
    """Write the alert stream to the output Kafka topic.

    Uses the `update` output mode, which emits a row every time an existing
    window's aggregate is updated — appropriate for sliding windows with
    watermarks.

    Args:
        formatted_stream: The streaming DataFrame with the `value` column.

    Returns:
        A StreamingQuery handle that keeps the stream running.
    """
    return (
        formatted_stream.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", KAFKA_JAAS)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", "/tmp/spark_checkpoints/hw06_alerts")
        .outputMode("update")
        .start()
    )


if __name__ == "__main__":
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading from Kafka topic: '{INPUT_TOPIC}'")
    print(f"Writing alerts to Kafka topic: '{OUTPUT_TOPIC}'")
    print(f"Loading alert conditions from: '{ALERTS_CSV_PATH}'")

    alerts_conditions_df = read_alerts_conditions(spark)

    print("\nLoaded alert conditions:")
    alerts_conditions_df.show(truncate=False)

    raw_kafka_stream = read_kafka_stream(spark)
    parsed_stream = parse_sensor_stream(raw_kafka_stream)
    windowed_averages = compute_window_averages(parsed_stream)
    triggered_alerts = apply_alert_conditions(windowed_averages, alerts_conditions_df)
    kafka_ready_stream = format_for_kafka(triggered_alerts)

    query = write_alerts_to_kafka(kafka_ready_stream)

    print("\nStream processing started. Waiting for data... Press Ctrl+C to stop.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping stream processor...")
        query.stop()
        spark.stop()
        print("Done.")
