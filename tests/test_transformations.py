from q1_pipeline.account_snapshot import transform_transactions
from q1_pipeline.utils import create_spark_session


def test_transform_transactions():
    spark = create_spark_session()

    data = [
        ("TXN1", "ACC1", "CREDIT", 100000.0),
        ("TXN2", "ACC1", "DEBIT", -50000.0),
        ("TXN3", "ACC1", "PAYMENT", 75000.0),
        ("TXN4", "ACC1", "TRANSFER_IN", 200000.0),
        ("TXN5", "ACC1", "TRANSFER_OUT", -25000.0),
        ("TXN6", "ACC1", "FEE", -10000.0),
    ]

    columns = [
        "txn_id",
        "account_id",
        "txn_type",
        "amount",
    ]

    df = spark.createDataFrame(data, columns)

    result = transform_transactions(df)

    actual = {
        row["txn_type"]: row["signed_amount"]
        for row in result.select("txn_type", "signed_amount").collect()
    }

    assert actual["CREDIT"] == 100000.0
    assert actual["DEBIT"] == -50000.0
    assert actual["PAYMENT"] == -75000.0
    assert actual["TRANSFER_IN"] == 200000.0
    assert actual["TRANSFER_OUT"] == -25000.0
    assert actual["FEE"] == -10000.0

    spark.stop()
