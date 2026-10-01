from pyspark.sql import SparkSession


DATASETS = [
    "customers",
    "accounts",
    "transactions",
    "credit_scores",
    "app_events",
    "support_tickets",
    "acquisition_channels",
]


def create_spark_session():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("InsigniaDataProfiling")
        .getOrCreate()
    )


def profile_dataset(spark, dataset_name):
    path = f"data/{dataset_name}.csv"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

    print("\n" + "=" * 80)
    print(f"DATASET: {dataset_name}")
    print("=" * 80)

    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")
    print(f"Column names: {df.columns}")

    print("\nSchema:")
    df.printSchema()

    print("\nSample:")
    df.show(3, truncate=False)


def main():
    spark = create_spark_session()

    try:
        for dataset_name in DATASETS:
            profile_dataset(spark, dataset_name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
