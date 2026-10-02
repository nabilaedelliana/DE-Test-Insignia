# Q1 Account Snapshot Pipeline Design

## 1. Objective

Build a production-oriented nightly account snapshot pipeline using PySpark and Airflow.

The pipeline processes account and transaction data incrementally, maintains cumulative transaction metrics at account grain, and writes an idempotent snapshot partitioned by `snapshot_date`.

Target grain:

> One row per `account_id` per `snapshot_date`.

---

## 2. Source Data

The pipeline uses:

- `accounts.csv`
- `transactions.csv`

The provided dataset contains:

- 7,954 accounts
- 1,991,349 transactions

The account-to-transaction relationship is:

```text
customers
    |
    | 1:N
    v
accounts
    |
    | 1:N
    v
transactions
```

The pipeline starts from the account table and performs a left join with aggregated transaction data. This preserves accounts that have no transactions.

---

## 3. Problems With the Original Nightly Pipeline

The original design has several production issues:

1. Full transaction history is repeatedly scanned every night.
2. Transactions since a fixed date (`2026-01-01`) are repeatedly reprocessed.
3. There is no persistent watermark for incremental processing.
4. The pipeline overwrites the complete output instead of replacing only the required snapshot partition.
5. There is no explicit idempotency strategy.
6. Credentials should not be hardcoded in application code.
7. There are insufficient data-quality assertions.
8. There is no explicit transaction/account referential-integrity check.
9. The pipeline does not clearly separate extraction, transformation, validation, and loading.
10. There is no orchestration dependency to trigger downstream credit scoring only after a successful snapshot.
11. Failure handling and retry behavior are not explicitly defined.
12. Historical balance reconciliation cannot be reliably performed from the supplied dataset because `accounts.balance` represents a current balance snapshot rather than historical balance records.

---

## 4. Incremental Loading Strategy

The pipeline uses `transactions.txn_date` as the transaction watermark.

For each execution:

```text
previous_watermark < txn_date <= current_source_watermark
```

The current source watermark is calculated as:

```text
MAX(transactions.txn_date)
```

The successful watermark is persisted only after the snapshot has been successfully written.

This prevents a failed pipeline run from advancing the watermark incorrectly.

### Initial Load

For the first execution, there is no previous watermark.

Therefore:

```text
txn_date <= current_source_watermark
```

is processed.

### Subsequent Loads

For subsequent executions:

```text
previous_watermark < txn_date <= current_source_watermark
```

is processed.

This avoids reprocessing transactions that were already included in the previous successful run.

---

## 5. Transaction Transformation

Transaction types are normalized into signed transaction amounts.

Credit-like transactions:

```text
CREDIT
TRANSFER_IN
```

are treated as positive amounts.

Debit-like transactions:

```text
DEBIT
TRANSFER_OUT
FEE
PAYMENT
```

are treated as negative amounts.

The transformed transactions are aggregated to account grain:

```text
account_id
transaction_count
net_transaction_amount
```

The aggregation is performed before joining to the account dataset to reduce the amount of data involved in the account-level join.

---

## 6. Incremental State Management

The pipeline maintains cumulative account transaction state.

For each account:

```text
cumulative_transaction_count
=
previous_transaction_count
+
incremental_transaction_count
```

and:

```text
cumulative_net_transaction_amount
=
previous_net_transaction_amount
+
incremental_net_transaction_amount
```

The latest successful snapshot is used as the previous cumulative state.

This allows new transactions to be incrementally added without recalculating the complete transaction history.

---

## 7. Snapshot Construction

The account table is left joined with the cumulative transaction state.

This ensures accounts without transactions remain in the snapshot.

The resulting snapshot contains:

```text
account_id
customer_id
account_type
product_name
opened_date
status
balance
credit_limit
interest_rate
transaction_count
net_transaction_amount
snapshot_date
```

The output grain is:

```text
account_id + snapshot_date
```

---

## 8. Data Quality Checks

The pipeline performs the following checks before writing the snapshot.

### 8.1 Account Row Count

The snapshot row count must equal the source account row count.

For the provided dataset:

```text
Expected accounts: 7,954
Expected snapshot rows: 7,954
```

### 8.2 Duplicate Account Check

The snapshot must not contain duplicate `account_id` values within a snapshot.

### 8.3 Transaction/Account Integrity

Aggregated transaction accounts are checked against the account dimension using a left-anti join.

If an aggregated transaction references an account that does not exist, the pipeline fails.

The current dataset passes this check with zero orphan transaction accounts.

### 8.4 Balance Reconciliation Limitation

The assessment requires balance reconciliation.

However, the supplied `accounts.balance` field represents a current account balance snapshot. The supplied dataset does not provide historical opening balances, previous account balances, or account balance CDC records.

Therefore, an exact historical reconciliation such as:

```text
opening_balance
+ credits
- debits
=
closing_balance
```

cannot be independently validated from the supplied data.

The production design should therefore consume a source containing either:

- historical account balance snapshots,
- opening and closing balances, or
- balance CDC events.

With that additional source, the pipeline can implement a tolerance-based reconciliation between calculated and source balances.

The pipeline does not fabricate this reconciliation from incomplete source data.

---

## 9. Idempotency

Snapshot output is partitioned by:

```text
snapshot_date
```

Example:

```text
output/account_snapshots/
â””â”€â”€ snapshot_date=2026-02-18/
    â””â”€â”€ *.parquet
```

Dynamic partition overwrite is used so that rerunning the same snapshot date replaces that partition rather than creating duplicate snapshot records.

The watermark is persisted only after the output write succeeds.

Therefore, a failed run does not incorrectly advance the incremental state.

---

## 10. Output Format

The local implementation writes Parquet.

Production implementations can use a transactional lakehouse format such as Delta Lake or Apache Iceberg when supported by the target platform.

Partitioning strategy:

```text
snapshot_date
```

This supports efficient retrieval of snapshots for a specific business date and avoids repeatedly scanning all historical partitions.

---

## 11. Secret Management

No database credentials or secrets are hardcoded into the pipeline.

For local development, configuration can be supplied through environment variables.

For production, credentials should be retrieved from a managed secret store such as:

```text
AWS Secrets Manager
```

or an equivalent platform-native secret management service.

Secrets should never be committed to Git.

---

## 12. Airflow Orchestration

Airflow orchestrates the pipeline using the following dependency:

```text
account_snapshot
        |
        | success
        v
trigger_credit_scoring
```

The account snapshot task is configured with:

```text
retries = 3
retry_delay = 1 minute
```

A failure callback logs an alert after the task exhausts its retries.

The credit scoring workflow is triggered only after the account snapshot task succeeds.

In this assessment implementation, `trigger_credit_scoring` is an orchestration placeholder. In production it would invoke the actual credit scoring workflow or job.


### Credit Scoring 30-Minute SLA

The production pipeline must ensure that the credit scoring workflow starts within 30 minutes after the account snapshot succeeds.

The SLA is monitored using the snapshot task completion timestamp and the downstream credit scoring start timestamp:

```text
credit_scoring_start_time - snapshot_success_time <= 30 minutes

Airflow should record both timestamps as task metadata or execution metrics.
If the downstream workflow has not started within the 30-minute SLA window, the monitoring layer should raise an SLA-breach alert to the data engineering/on-call channel.
The monitoring flow is:

Account Snapshot SUCCESS
        |
        v
Record snapshot_success_time
        |
        v
Trigger Credit Scoring
        |
        v
Record credit_scoring_start_time
        |
        v
Check elapsed time
        |
   +----+----+
   |         |
 <= 30 min  > 30 min
   |         |
   v         v
  PASS    SLA BREACH
             |
             v
       Alert / Incident

The current assessment implementation does not execute the actual credit scoring system, so the SLA monitoring is documented as a production requirement rather than claimed as an executed end-to-end measurement.
---

## 13. Failure Handling

Pipeline failures are logged with the exception stack trace and re-raised so that Airflow can mark the task as failed.

The sequence is:

```text
Pipeline failure
      |
      v
Airflow retry
      |
      +---- retry 1
      |
      +---- retry 2
      |
      +---- retry 3
      |
      v
Failure callback / alert
```

The downstream credit scoring task is not executed when the snapshot task fails.

---

## 14. Testing

The pipeline includes automated tests for:

- transaction sign transformation,
- account-level transaction aggregation,
- incremental state merging.

The test suite is executed using:

```bash
python -m pytest -q
```

The current test suite passes all implemented tests.

The pipeline has also been executed successfully through Airflow using the local dataset.

---

## 15. Production Architecture

A production deployment can follow:

```text
Oracle Core Banking
        |
        v
   CDC / Batch Ingestion
        |
        v
     Bronze
        |
        v
     Silver
        |
        v
Account Snapshot
        |
        v
      Gold
        |
        +--------------------+
        |                    |
        v                    v
Customer / BI Metrics   Credit Scoring
```

Airflow or a managed orchestration service controls dependencies, retries, monitoring, and downstream triggering.

The local implementation demonstrates the core processing logic without requiring a full cloud deployment.

---

## 16. Technology Choices

| Component | Technology | Reason |
|---|---|---|
| Processing | PySpark | Distributed processing and scalable transformation |
| Orchestration | Apache Airflow | Scheduling, dependencies, retries, monitoring |
| Storage | Parquet | Columnar storage and partitioning |
| Testing | pytest | Automated unit testing |
| Version control | Git | Reproducibility and collaboration |
| Production secrets | AWS Secrets Manager | Secure credential management |
| Production lakehouse | Delta Lake / Iceberg | Transactional and scalable analytical storage |

---

## 17. Current Dataset Limitation

The supplied assessment dataset is a static CSV dataset.

It does not fully represent the production architecture described in the assessment, particularly:

- continuously arriving Oracle CDC data,
- historical account balance snapshots,
- production secret stores,
- real downstream credit-scoring execution.

The implementation therefore focuses on demonstrating the required engineering patterns:

- incremental processing,
- watermarking,
- cumulative state,
- partitioned snapshots,
- idempotency,
- data-quality validation,
- logging,
- Airflow orchestration,
- retries,
- downstream dependency management,
- and testability.

