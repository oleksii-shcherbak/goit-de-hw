import os
import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def clean_text(text):
    return re.sub(r"[^a-zA-Z0-9,.'\"\\s]", "", str(text))


def main():
    import pyspark
    os.environ["SPARK_HOME"] = os.path.join(os.path.dirname(pyspark.__file__))
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    clean_text_udf = udf(clean_text, StringType())

    for table in ["athlete_bio", "athlete_event_results"]:
        df = spark.read.parquet(f"/tmp/bronze/{table}")
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(col_name, clean_text_udf(df[col_name]))
        df = df.dropDuplicates()
        output_path = f"/tmp/silver/{table}"
        df.write.mode("overwrite").parquet(output_path)
        spark.read.parquet(output_path).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
