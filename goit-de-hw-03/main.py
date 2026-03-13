import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def create_spark_session() -> SparkSession:
    """Create and return a local SparkSession.

    Returns:
        SparkSession: Configured Spark session for local execution.
    """
    return (
        SparkSession.builder
        .appName("goit-de-hw-03")
        .master("local[*]")
        .getOrCreate()
    )


def load_csv(spark: SparkSession, filename: str):
    """Load a CSV file from the data directory into a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        filename: Name of the CSV file (e.g. "users.csv").

    Returns:
        DataFrame: Loaded Spark DataFrame with inferred schema and header.
    """
    path = os.path.join(DATA_DIR, filename)
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )


def main() -> None:
    """Execute all six homework steps and display results."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------------
    # Step 1 - Load CSV files
    # ------------------------------------------------------------------
    print("=" * 60)
    print("STEP 1 — Loading CSV files")
    print("=" * 60)

    users_df = load_csv(spark, "users.csv")
    purchases_df = load_csv(spark, "purchases.csv")
    products_df = load_csv(spark, "products.csv")

    print("\nusers_df:")
    users_df.show(5)

    print("purchases_df:")
    purchases_df.show(5)

    print("products_df:")
    products_df.show(5)

    # ------------------------------------------------------------------
    # Step 2 - Remove rows with missing values
    # ------------------------------------------------------------------
    print("=" * 60)
    print("STEP 2 — Cleaning: dropping rows with null values")
    print("=" * 60)

    users_clean = users_df.dropna()
    purchases_clean = purchases_df.dropna()
    products_clean = products_df.dropna()

    print(f"users:     {users_df.count()} rows → {users_clean.count()} after cleaning")
    print(f"purchases: {purchases_df.count()} rows → {purchases_clean.count()} after cleaning")
    print(f"products:  {products_df.count()} rows → {products_clean.count()} after cleaning")

    # ------------------------------------------------------------------
    # Step 3 - Total purchase amount per product category
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 3 — Total purchase amount per product category")
    print("=" * 60)

    purchases_with_products = purchases_clean.join(
        products_clean, on="product_id", how="inner"
    )

    total_by_category = (
        purchases_with_products
        .withColumn("total_cost", F.col("quantity") * F.col("price"))
        .groupBy("category")
        .agg(F.round(F.sum("total_cost"), 2).alias("total_spend"))
        .orderBy(F.col("total_spend").desc())
    )

    total_by_category.show()

    # ------------------------------------------------------------------
    # Step 4 - Total purchase amount per category for users aged 18–25
    # ------------------------------------------------------------------
    print("=" * 60)
    print("STEP 4 — Total purchase amount per category (age 18–25)")
    print("=" * 60)

    young_users = users_clean.filter(
        (F.col("age") >= 18) & (F.col("age") <= 25)
    )

    young_purchases = purchases_clean.join(
        young_users, on="user_id", how="inner"
    ).join(
        products_clean, on="product_id", how="inner"
    )

    young_by_category = (
        young_purchases
        .withColumn("total_cost", F.col("quantity") * F.col("price"))
        .groupBy("category")
        .agg(F.round(F.sum("total_cost"), 2).alias("young_spend"))
        .orderBy(F.col("young_spend").desc())
    )

    young_by_category.show()

    # ------------------------------------------------------------------
    # Step 5 - Share (%) of each category in total spending for 18–25
    # ------------------------------------------------------------------
    print("=" * 60)
    print("STEP 5 — Category share (%) of spending for age 18–25")
    print("=" * 60)

    total_young_spend = young_by_category.agg(
        F.sum("young_spend").alias("grand_total")
    ).collect()[0]["grand_total"]

    young_share = (
        young_by_category
        .withColumn(
            "percentage",
            F.round((F.col("young_spend") / total_young_spend) * 100, 2)
        )
        .orderBy(F.col("percentage").desc())
    )

    young_share.show()

    # ------------------------------------------------------------------
    # Step 6 - Top 3 categories by spending share for users aged 18–25
    # ------------------------------------------------------------------
    print("=" * 60)
    print("STEP 6 — Top 3 categories by spending share (age 18–25)")
    print("=" * 60)

    top3 = young_share.orderBy(F.col("percentage").desc()).limit(3)
    top3.show()

    spark.stop()


if __name__ == "__main__":
    main()