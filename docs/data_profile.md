# Data Profile

## Dataset Overview

| Dataset | Rows | Columns |
|---|---:|---:|
| customers | 5,000 | 11 |
| accounts | 7,954 | 9 |
| transactions | 1,991,349 | 9 |
| credit_scores | 7,452 | 7 |
| app_events | 500,000 | 8 |
| support_tickets | 16,953 | 8 |
| acquisition_channels | 5,000 | 4 |

## Key Observations

- `accounts.balance` is a current account balance field.
- `transactions` contains transaction-level records linked to accounts through `account_id`.
- `transactions.txn_date` is a timestamp.
- `customers` contains PII fields including NIK, phone, and email.
- `credit_scores.features_used` is stored as a JSON-formatted string.
- `app_events.event_timestamp` is a timestamp suitable for event-time processing.
- Raw CSV files are excluded from Git through `.gitignore` because the dataset contains customer information.

## Q1 Design Implications

- Transaction data should be aggregated before joining to account-level data.
- Incremental processing should use a transaction watermark rather than reprocessing the full transaction history.
- Snapshot output should be partitioned by `snapshot_date`.
- Data quality checks should include row-count sanity checks and balance reconciliation.
