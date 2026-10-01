# Q1. Production-Grade Account Snapshot Pipeline


## 1. Objective

Build a production grade account snapshot pipeline that incrementally processes transaction data and produces an account-level snapshot partitioned by `snapshot_date`.

The pipeline must support:

- Incremental processing
- Account-level aggregation
- Data quality validation
- Idempotent execution
- Partitioned output
- Secure credential management
- Logging and monitoring
- Downstream credit scoring orchestration

## 2. Data Grain

### Source relationships

customers
    |
    +----< accounts
                |
                +----< transactions

### Target grain

The target snapshot has one row per:

`account_id + snapshot_date`

`customer_id` is retained as an attribute/foreign key for customer-level aggregation in downstream analytical models.

## 3. Source Tables

### Accounts

Primary business key:

`account_id`

Customer relationship:

`customer_id`

The `accounts` table contains the current account balance and account attributes.

### Transactions

Primary business key:

`txn_id`

Relationship to accounts:

`transactions.account_id -> accounts.account_id`

Transactions are aggregated before joining to accounts to avoid row multiplication.

## 4. Account Coverage

The supplied dataset contains:

- 7,954 accounts
- 1,991,349 transactions
- 6,271 accounts with transactions
- 1,683 accounts without transactions

The pipeline uses a LEFT JOIN from accounts to aggregated transactions so that accounts without transactions are not dropped from the snapshot.

## 5. Transformation Strategy

### Step 1. Extract accounts

Read the account source.

### Step 2. Extract incremental transactions

Read only transactions newer than the stored processing watermark.

The watermark should be based on the transaction timestamp/date rather than a hard-coded date.

### Step 3. Filter valid transactions

Apply business rules such as transaction status and valid transaction types.

### Step 4. Normalize transaction sign

Transaction types are converted into signed amounts.

Conceptually:

- CREDIT / TRANSFER_IN -> positive
- DEBIT / TRANSFER_OUT / PAYMENT / FEE -> negative

### Step 5. Aggregate transactions

Aggregate transactions at:

`account_id`

before joining to accounts.

Example metrics:

- transaction_count
- net_transaction_amount
- credit_amount
- debit_amount

### Step 6. Join to accounts

Use:

`accounts LEFT JOIN aggregated_transactions`

using:

`accounts.account_id = aggregated_transactions.account_id`

### Step 7. Add snapshot date

Each pipeline execution creates a snapshot identified by:

`snapshot_date`

### Step 8. Data quality validation

Validate the generated snapshot before publishing it.

### Step 9. Write output

Write the snapshot partitioned by:

`snapshot_date`

## 6. Incremental Processing

The pipeline should not repeatedly scan the entire transaction history.

A production implementation should maintain a watermark containing the latest successfully processed transaction timestamp.

Example metadata:

- pipeline_name
- run_id
- watermark_start
- watermark_end
- snapshot_date
- row_count
- dq_status
- execution_time

A new run processes:

`watermark_start < txn_timestamp <= watermark_end`

The watermark is advanced only after the pipeline completes successfully.

## 7. Idempotency

The same `snapshot_date` must be safely re-runnable.

The pipeline should replace only the target partition corresponding to the current snapshot date rather than overwriting the entire dataset.

Conceptually:

`output/snapshot_date=YYYY-MM-DD/`

Rerunning the same snapshot date should replace only that partition.

## 8. Data Quality Checks

The pipeline should validate:

### Row count sanity

Expected account snapshot row count should be close to the number of source accounts.

The baseline for the supplied dataset is:

`7,954 accounts`

### Account uniqueness

There must be no duplicate:

`account_id + snapshot_date`

### Null checks

Required identifiers must not be null:

- account_id
- customer_id
- snapshot_date

### Balance reconciliation

The pipeline should validate account balance reconciliation where the source data provides sufficient historical information.

The supplied `accounts.balance` field is a current balance snapshot, so exact historical balance reconciliation cannot be reconstructed from the supplied tables alone.

In production, historical account snapshots or CDC/opening-balance data would be required for exact reconciliation.

## 9. Secret Management

Database credentials must not be hard coded in source code.

Production implementation should retrieve credentials from a secret manager, such as:

- AWS Secrets Manager
- Databricks Secret Scope

Local development may use environment variables.

Example:

- DB_HOST
- DB_PORT
- DB_NAME
- DB_USER
- DB_PASSWORD

Secrets must not be committed to Git.

## 10. Logging

The pipeline should log:

- Pipeline start
- Run ID
- Source extraction
- Source row counts
- Watermark range
- Transformation completion
- Data quality results
- Output partition
- Output row count
- Pipeline completion
- Failure details

## 11. Orchestration

Airflow will orchestrate the pipeline.

Conceptually:

Extract & Transform
        |
        v
Data Quality Checks
        |
     SUCCESS
        |
        v
Write Account Snapshot
        |
     SUCCESS
        |
        v
Trigger Credit Scoring

The snapshot task should be retried automatically.

After three failed attempts, the workflow should alert the responsible team.

Credit scoring should only be triggered after the snapshot pipeline succeeds.

## 12. Technology

Implementation:

- PySpark for transformation
- Airflow for orchestration
- Parquet/Delta-style partitioned storage for the snapshot output
- pytest for automated testing
- Git for version control

Production cloud implementation can use managed services such as:

- Databricks
- AWS Secrets Manager
- Amazon S3
- Databricks Workflows

The assessment implementation can remain local without requiring deployment to AWS or Databricks.

## 13. Key Design Decisions

### Aggregate before joining

Transactions are aggregated by `account_id` before joining with accounts.

This avoids multiplying account rows by the number of transactions.

### LEFT JOIN

Accounts are the driving dataset.

A LEFT JOIN preserves accounts that have no transactions.

### Incremental processing

A watermark prevents repeatedly processing the complete transaction history.

### Partitioned snapshots

Output is partitioned by `snapshot_date` to support historical snapshots and efficient downstream reads.

### Idempotent execution

Only the target snapshot partition is replaced during a rerun.

### Secure credentials

Credentials are retrieved from environment variables locally and a secret manager in production.

