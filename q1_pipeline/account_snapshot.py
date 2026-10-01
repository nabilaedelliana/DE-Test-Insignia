import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from q1_pipeline.config import (
    ACCOUNTS_FILE,
    TRANSACTIONS_FILE,
    VALID_TRANSACTION_STATUSES,
    SNAPSHOT_OUTPUT_DIR,
    PIPELINE_NAME,
    SNAPSHOT_PARTITION_COLUMN
)
from q1_pipeline.utils import (
    configure_logging,
    create_spark_session,
    load_watermark,
    save_watermark,
)

logger = logging.getLogger(__name__)


def extract_accounts(spark) -> DataFrame:

    # Read the accounts source data.

    logger.info("Reading accounts from %s", ACCOUNTS_FILE)

    accounts_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(ACCOUNTS_FILE))
    )

    logger.info("Accounts extracted: %s rows", accounts_df.count())

    return accounts_df


def extract_transactions(spark) -> DataFrame:
    # Read the transactions source data.

    logger.info("Reading transactions from %s", TRANSACTIONS_FILE)

    transactions_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(TRANSACTIONS_FILE))
    )

    logger.info(
        "Transactions extracted: %s rows",
        transactions_df.count(),
    )

    return transactions_df

def get_transaction_watermark(transactions_df: DataFrame):
    """
    Get the maximum transaction timestamp from the source data.

    This value can be persisted as the new watermark only after
    the pipeline completes successfully.
    """

    watermark = (
        transactions_df
        .select(F.max("txn_date").alias("max_txn_date"))
        .collect()[0]["max_txn_date"]
    )

    if watermark is None:
        raise ValueError("No transaction timestamp found in source data")

    logger.info("Source transaction watermark: %s", watermark)

    return watermark

def get_incremental_transactions(
    transactions_df: DataFrame,
    previous_watermark=None,
) -> tuple[DataFrame, object]:
    """
    Return transactions within the current incremental window.

    Window:
        previous_watermark < txn_date <= current_watermark

    For an initial load, previous_watermark is None and all
    eligible transactions are processed.

    The returned current watermark should only be persisted
    after the downstream pipeline succeeds.
    """

    current_watermark = get_transaction_watermark(transactions_df)

    if previous_watermark is None:
        logger.info(
            "Initial load detected. Processing transactions up to %s",
            current_watermark,
        )

        incremental_df = transactions_df.filter(
            F.col("txn_date") <= F.lit(current_watermark)
        )
    else:
        logger.info(
            "Incremental load: %s < txn_date <= %s",
            previous_watermark,
            current_watermark,
        )

        incremental_df = transactions_df.filter(
            (F.col("txn_date") > F.lit(previous_watermark))
            & (F.col("txn_date") <= F.lit(current_watermark))
        )

    logger.info(
        "Transactions in incremental window: %s",
        incremental_df.count(),
    )

    return incremental_df, current_watermark


def filter_transactions(
    transactions_df: DataFrame,
    watermark_start=None,
    watermark_end=None,
) -> DataFrame:

    # Filter transactions based on business status and optional watermark.


    logger.info(
        "Filtering transactions with valid statuses: %s",
        VALID_TRANSACTION_STATUSES,
    )

    filtered_df = transactions_df.filter(
        F.col("status").isin(list(VALID_TRANSACTION_STATUSES))
    )

    if watermark_start is not None:
        filtered_df = filtered_df.filter(
            F.col("txn_date") > F.lit(watermark_start)
        )

    if watermark_end is not None:
        filtered_df = filtered_df.filter(
            F.col("txn_date") <= F.lit(watermark_end)
        )

    logger.info(
        "Transactions after filtering: %s rows",
        filtered_df.count(),
    )

    return filtered_df


def transform_transactions(transactions_df: DataFrame) -> DataFrame:
    """
    Normalize transaction amounts into a common signed_amount convention.

    Source dataset convention:
    - CREDIT / TRANSFER_IN: positive amount
    - DEBIT / TRANSFER_OUT / FEE: negative amount
    - PAYMENT: positive amount, but represents an outgoing payment

    Therefore:
    - PAYMENT is converted to negative.
    - Other transaction types preserve the source sign.
    """

    transformed_df = transactions_df.withColumn(
        "signed_amount",
        F.when(
            F.col("txn_type") == "PAYMENT",
            -F.abs(F.col("amount")),
        ).otherwise(
            F.col("amount")
        ),
    )

    return transformed_df


def aggregate_transactions(transactions_df: DataFrame) -> DataFrame:
    """
    Aggregate transactions to account level.

    Output grain:
    one row per account_id.
    """

    aggregated_df = (
        transactions_df
        .groupBy("account_id")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("signed_amount").alias("net_transaction_amount"),
        )
    )

    logger.info(
        "Aggregated transactions for %s accounts",
        aggregated_df.count(),
    )

    return aggregated_df

def build_snapshot(accounts_df: DataFrame, aggregated_transactions_df: DataFrame) -> DataFrame:
    """
    Build account-level snapshot.

    Output grain:
    one row per account_id.
    """

    snapshot_df = (
        accounts_df.alias("a")
        .join(
            aggregated_transactions_df.alias("t"),
            F.col("a.account_id") == F.col("t.account_id"),
            "left",
        )
        .select(
            F.col("a.account_id"),
            F.col("a.customer_id"),
            F.col("a.account_type"),
            F.col("a.product_name"),
            F.col("a.opened_date"),
            F.col("a.status"),
            F.col("a.balance"),
            F.col("a.credit_limit"),
            F.col("a.interest_rate"),
            F.coalesce(
                F.col("t.transaction_count"),
                F.lit(0),
            ).alias("transaction_count"),
            F.coalesce(
                F.col("t.net_transaction_amount"),
                F.lit(0.0),
            ).alias("net_transaction_amount"),
        )
    )

    logger.info(
        "Account snapshot rows: %s",
        snapshot_df.count(),
    )

    return snapshot_df


def add_snapshot_date(
    snapshot_df: DataFrame,
    snapshot_date: str,
) -> DataFrame:
    """
    Add a deterministic snapshot_date to the account snapshot.

    snapshot_date is supplied by the pipeline/orchestrator
    rather than generated implicitly inside the transformation.
    """

    return snapshot_df.withColumn(
        "snapshot_date",
        F.to_date(F.lit(snapshot_date)),
    )

def load_previous_account_state(spark) -> DataFrame | None:
    """
    Load the latest account transaction state from the previous snapshot.

    Returns None when no previous snapshot exists.
    """
    if not SNAPSHOT_OUTPUT_DIR.exists():
        return None

    previous_snapshot_df = spark.read.parquet(str(SNAPSHOT_OUTPUT_DIR))

    if previous_snapshot_df.rdd.isEmpty():
        return None

    latest_snapshot_date = previous_snapshot_df.agg(
        F.max("snapshot_date").alias("latest_snapshot_date")
    ).collect()[0]["latest_snapshot_date"]

    return (
        previous_snapshot_df
        .filter(F.col("snapshot_date") == F.lit(latest_snapshot_date))
        .select(
            "account_id",
            "transaction_count",
            "net_transaction_amount",
        )
    )


def merge_account_state(
    accounts_df: DataFrame,
    previous_state_df: DataFrame | None,
    incremental_state_df: DataFrame,
) -> DataFrame:
    """
    Merge previous cumulative account state
    with the current incremental transaction delta.
    """

    incremental_df = incremental_state_df.select(
        "account_id",
        F.col("transaction_count").alias(
            "incremental_transaction_count"
        ),
        F.col("net_transaction_amount").alias(
            "incremental_net_transaction_amount"
        ),
    )

    if previous_state_df is None:
        cumulative_state_df = incremental_df.select(
            "account_id",
            F.col("incremental_transaction_count").alias(
                "transaction_count"
            ),
            F.col("incremental_net_transaction_amount").alias(
                "net_transaction_amount"
            ),
        )

    else:
        previous_df = previous_state_df.select(
            "account_id",
            F.col("transaction_count").alias(
                "previous_transaction_count"
            ),
            F.col("net_transaction_amount").alias(
                "previous_net_transaction_amount"
            ),
        )

        cumulative_state_df = (
            previous_df
            .join(
                incremental_df,
                on="account_id",
                how="full_outer",
            )
            .select(
                F.coalesce(
                    F.col("account_id"),
                    F.lit(None),
                ).alias("account_id"),
                (
                    F.coalesce(
                        F.col("previous_transaction_count"),
                        F.lit(0),
                    )
                    + F.coalesce(
                        F.col("incremental_transaction_count"),
                        F.lit(0),
                    )
                ).alias("transaction_count"),
                (
                    F.coalesce(
                        F.col("previous_net_transaction_amount"),
                        F.lit(0.0),
                    )
                    + F.coalesce(
                        F.col("incremental_net_transaction_amount"),
                        F.lit(0.0),
                    )
                ).alias("net_transaction_amount"),
            )
        )

    return (
        accounts_df
        .join(
            cumulative_state_df,
            on="account_id",
            how="left",
        )
        .select(
            accounts_df["*"],
            F.coalesce(
                F.col("transaction_count"),
                F.lit(0),
            ).alias("transaction_count"),
            F.coalesce(
                F.col("net_transaction_amount"),
                F.lit(0.0),
            ).alias("net_transaction_amount"),
        )
    )




def validate_snapshot(
    snapshot_df: DataFrame,
    expected_account_count: int,
) -> None:
    """
    Validate basic account snapshot data quality.
    """

    snapshot_count = snapshot_df.count()

    logger.info(
        "Validating snapshot row count: expected=%s actual=%s",
        expected_account_count,
        snapshot_count,
    )

    if snapshot_count != expected_account_count:
        raise ValueError(
            f"Snapshot row count mismatch: "
            f"expected {expected_account_count}, "
            f"got {snapshot_count}"
        )

    duplicate_accounts = (
        snapshot_df
        .groupBy("account_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_accounts > 0:
        raise ValueError(
            f"Snapshot contains {duplicate_accounts} duplicate account_id values"
        )

    logger.info("Snapshot data quality validation passed")

def write_snapshot(
    snapshot_df: DataFrame,
) -> None:
    """
    Write account snapshots partitioned by snapshot_date.

    The partition for the current snapshot date is replaced,
    making reruns for the same snapshot_date idempotent.
    """

    snapshot_dates = [
        row["snapshot_date"]
        for row in snapshot_df.select("snapshot_date").distinct().collect()
    ]

    if len(snapshot_dates) != 1:
        raise ValueError(
            f"Expected exactly one snapshot_date, found: {snapshot_dates}"
        )

    snapshot_date = snapshot_dates[0]

    logger.info(
        "Writing snapshot for snapshot_date=%s to %s",
        snapshot_date,
        SNAPSHOT_OUTPUT_DIR,
    )

    (
        snapshot_df
        .write
        .option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .partitionBy(SNAPSHOT_PARTITION_COLUMN)
        .parquet(str(SNAPSHOT_OUTPUT_DIR))
    )

    logger.info(
        "Snapshot successfully written for snapshot_date=%s",
        snapshot_date,
    )


def run_pipeline(snapshot_date: str) -> None:
    """
    Execute the account snapshot pipeline.

    The Flow:
        1. Extract source data
        2. Determine incremental transaction window
        3. Apply business filters
        4. Transform transaction amounts
        5. Aggregate transactions to account grain
        6. Build account snapshot
        7. Add snapshot date
        8. Merge Prev Date  with incremental trx date
        9. Run data quality validations
        10. Write idempotent snapshot partition
        11. Persist watermark only after successful write
    """

    logger.info(
        "Starting pipeline: %s | snapshot_date=%s",
        PIPELINE_NAME,
        snapshot_date,
    )

    spark = create_spark_session()

    try:
        # 1. Extract
        accounts_df = extract_accounts(spark)
        transactions_df = extract_transactions(spark)

        expected_account_count = accounts_df.count()

        # 2. Load previous watermark
        state_file = SNAPSHOT_OUTPUT_DIR.parent / "pipeline_state.json"

        previous_watermark = load_watermark(
            state_file,
            PIPELINE_NAME,
        )

        # 3. Determine incremental transaction window
        incremental_transactions_df, current_watermark = (
            get_incremental_transactions(
                transactions_df,
                previous_watermark,
            )
        )

        # 4. Business filtering
        filtered_transactions_df = filter_transactions(
            incremental_transactions_df,
        )

        # 5. Transform
        transformed_transactions_df = transform_transactions(
            filtered_transactions_df,
        )

        # 6. Aggregate to account grain
        aggregated_transactions_df = aggregate_transactions(
            transformed_transactions_df,
        )

        # 7. Load previous cumulative account state
        previous_state_df = load_previous_account_state(spark)

        # 8. Merge previous state with incremental transaction state
        snapshot_df = merge_account_state(
            accounts_df,
            previous_state_df,
            aggregated_transactions_df,
        )

        snapshot_df = add_snapshot_date(
            snapshot_df,
            snapshot_date,
        )
        # 9. Data-quality validation
        validate_snapshot(
            snapshot_df,
            expected_account_count,
        )

        orphan_transactions = (
            aggregated_transactions_df
            .join(
                accounts_df.select("account_id"),
                "account_id",
                "left_anti",
            )
            .count()
        )

        if orphan_transactions > 0:
            raise ValueError(
                f"Found {orphan_transactions} orphan transaction accounts"
            )

        logger.info(
            "Transaction reconciliation passed:" "no orphan transaction accounts"
        )

        # 10. Write snapshot
        write_snapshot(snapshot_df)

        # 11. Persist watermark only after successful write
        save_watermark(
            state_file,
            PIPELINE_NAME,
            current_watermark,
        )

        logger.info(
            "Pipeline completed successfully: %s",
            PIPELINE_NAME,
        )

    except Exception:
        logger.exception(
            "Pipeline failed: %s",
            PIPELINE_NAME,
        )
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


def main():
    configure_logging()

    spark = create_spark_session()

    try:
        accounts_df = extract_accounts(spark)
        transactions_df = extract_transactions(spark)

        print("\nAccounts schema:")
        accounts_df.printSchema()

        print("\nAccounts sample:")
        accounts_df.show(5, truncate=False)

        print("\nTransactions schema:")
        transactions_df.printSchema()

        print("\nTransactions sample:")
        transactions_df.show(5, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

