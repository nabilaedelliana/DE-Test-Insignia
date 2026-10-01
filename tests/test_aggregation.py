from q1_pipeline.account_snapshot import aggregate_transactions
from q1_pipeline.utils import create_spark_session


def test_aggregate_transactions():
    spark = create_spark_session()

    data = [
        ("TXN1", "ACC1", 100000.0),
        ("TXN2", "ACC1", -50000.0),
        ("TXN3", "ACC1", -25000.0),
        ("TXN4", "ACC2", 200000.0),
    ]

    columns = [
        "txn_id",
        "account_id",
        "signed_amount",
    ]

    df = spark.createDataFrame(data, columns)

    result = aggregate_transactions(df)

    actual = {
        row["account_id"]: (
            row["transaction_count"],
            row["net_transaction_amount"],
        )
        for row in result.collect()
    }

    assert actual["ACC1"] == (3, 25000.0)
    assert actual["ACC2"] == (1, 200000.0)

    assert result.count() == 2
    assert result.select("account_id").distinct().count() == 2

    spark.stop()
