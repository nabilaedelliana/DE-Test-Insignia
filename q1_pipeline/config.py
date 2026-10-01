from pathlib import Path


# Project directories
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"


# Pipeline configuration
PIPELINE_NAME = "account_snapshot"
SPARK_MASTER = "local[2]"


# Source files
ACCOUNTS_FILE = DATA_DIR / "accounts.csv"
TRANSACTIONS_FILE = DATA_DIR / "transactions.csv"


# Output
SNAPSHOT_OUTPUT_DIR = OUTPUT_DIR / "account_snapshots"
SNAPSHOT_PARTITION_COLUMN = "snapshot_date"


# Transaction business rules
CREDIT_TYPES = {
    "CREDIT",
    "TRANSFER_IN",
}

DEBIT_TYPES = {
    "DEBIT",
    "TRANSFER_OUT",
    "PAYMENT",
    "FEE",
}

VALID_TRANSACTION_TYPES = CREDIT_TYPES | DEBIT_TYPES

VALID_TRANSACTION_STATUSES = {
    "COMPLETED",
}
