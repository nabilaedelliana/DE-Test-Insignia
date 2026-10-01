from q1_pipeline.account_snapshot import merge_account_state
from q1_pipeline.utils import create_spark_session


def test_merge_account_state():
    spark = create_spark_session()

    accounts_data = [
        ("ACC1", "CUST1"),
        ("ACC2", "CUST2"),
        ("ACC3", "CUST3"),
    ]

    accounts_df = spark.createDataFrame(
        accounts_data,
        ["account_id", "customer_id"],
    )

    previous_data = [
        ("ACC1", 10, 1_000_000.0),
        ("ACC2", 5, 2_000_000.0),
    ]

    previous_state_df = spark.createDataFrame(
        previous_data,
        [
            "account_id",
            "transaction_count",
            "net_transaction_amount",
        ],
    )

    incremental_data = [
        ("ACC1", 2, 300_000.0),
        ("ACC2", 1, -100_000.0),
        ("ACC3", 3, 500_000.0),
    ]

    incremental_state_df = spark.createDataFrame(
        incremental_data,
        [
            "account_id",
            "transaction_count",
            "net_transaction_amount",
        ],
    )

    result = merge_account_state(
        accounts_df,
        previous_state_df,
        incremental_state_df,
    )

    actual = {
        row["account_id"]: (
            row["transaction_count"],
            row["net_transaction_amount"],
        )
        for row in result.collect()
    }

    assert actual["ACC1"] == (12, 1_300_000.0)
    assert actual["ACC2"] == (6, 1_900_000.0)
    assert actual["ACC3"] == (3, 500_000.0)

    assert result.count() == 3

    spark.stop()
