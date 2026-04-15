import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, expr


def main():
    import pyspark
    os.environ["SPARK_HOME"] = os.path.join(os.path.dirname(pyspark.__file__))
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    athlete_bio = spark.read.parquet("/tmp/silver/athlete_bio")
    athlete_event_results = spark.read.parquet("/tmp/silver/athlete_event_results")

    athlete_bio = athlete_bio.withColumn(
        "height", expr("try_cast(regexp_replace(height, ',', '.') as double)")
    ).withColumn(
        "weight", expr("try_cast(regexp_replace(weight, ',', '.') as double)")
    )

    joined = athlete_event_results.drop("country_noc").join(athlete_bio, "athlete_id")

    gold_df = joined.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp"),
    )

    output_path = "/tmp/gold/avg_stats"
    gold_df.write.mode("overwrite").parquet(output_path)
    spark.read.parquet(output_path).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
