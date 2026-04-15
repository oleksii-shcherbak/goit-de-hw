import os
import sys

import requests
from pyspark.sql import SparkSession


def download_data(table_name):
    url = f"https://ftp.goit.study/neoversity/{table_name}.csv"
    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download {url}. Status: {response.status_code}")
    local_path = f"{table_name}.csv"
    with open(local_path, "wb") as f:
        f.write(response.content)
    return local_path


def main():
    import pyspark
    os.environ["SPARK_HOME"] = os.path.join(os.path.dirname(pyspark.__file__))
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table in ["athlete_bio", "athlete_event_results"]:
        local_csv = download_data(table)
        df = spark.read.csv(local_csv, header=True, inferSchema=True)
        output_path = f"/tmp/bronze/{table}"
        df.write.mode("overwrite").parquet(output_path)
        spark.read.parquet(output_path).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
