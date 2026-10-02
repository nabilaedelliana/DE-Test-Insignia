# Insignia Data Engineer Level 2 Assessment

Data Engineering take-home assessment for the Insignia AWS Business Unit.

This project implements:

- Q1. Production-grade incremental ETL pipeline using PySpark and Airflow.
- Q2. Customer Health Scorecard and Fraud Detection using PostgreSQL.
- Q3. Scalable data platform architecture supporting batch, streaming, ML, governance, and regulatory analytics.

## 1. Project Overview

The assessment scenario is based on NegaraBank, a digital-first bank that requires a scalable data platform supporting:

- High volume banking transactions.
- Customer and account analytics.
- Real time mobile clickstream processing.
- Credit scoring.
- Fraud detection.
- Regulatory reporting.
- Data governance and lineage.
- Batch and streaming workloads.

The implementation focuses on:

- Incremental processing.
- Idempotency.
- Data quality validation.
- Error handling.
- Logging.
- Orchestration.
- SQL optimization.
- Window functions and CTEs.
- Medallion architecture.
- Data lineage.
- PII protection.

## 2. Dataset

The provided dataset contains:

| Dataset | Records |
|---|---:|
| customers | 5,000 |
| accounts | 7,954 |
| transactions | 1,991,349 |
| credit_scores | 7,452 |
| app_events | 500,000 |
| support_tickets | 16,953 |
| acquisition_channels | 5,000 |

The raw CSV files are stored under:

    data/

Raw datasets are excluded from Git using `.gitignore`.

# Q1. Production-Grade Incremental ETL

## 3. Objective

The original account snapshot process reads account and transaction data, joins the datasets, aggregates transactions, and overwrites the output.

The implementation improves the process by introducing:

- Incremental transaction processing.
- Watermark-based loading.
- Historical snapshot partitions.
- Idempotent execution.
- Data quality assertions.
- Logging.
- Failure handling.
- Airflow orchestration.
- Downstream credit-scoring trigger.

## 4. Q1 Pipeline

The pipeline flow is:

    Transaction Source
           |
           v
    Incremental Watermark
           |
           v
    PySpark Transformation
           |
           v
    Transaction Aggregation
           |
           v
    Account Snapshot
           |
           v
    Data Quality Checks
           |
           v
    Partitioned Parquet
           |
           v
    Successful Snapshot
           |
           v
    Trigger Credit Scoring

The output is partitioned by:

    snapshot_date

Example:

    output/account_snapshots/
    └── snapshot_date=2026-02-18/
        └── part-*.snappy.parquet

## 5. Incremental Processing

The pipeline stores the last successfully processed transaction timestamp in:

    output/pipeline_state.json

Example:

    {
      "account_snapshot": {
        "last_successful_watermark": "2026-02-14T23:59:55"
      }
    }

Only transactions newer than the watermark are processed during subsequent runs.

The watermark is updated only after the output has been successfully written and validated.

This prevents a failed execution from incorrectly advancing the processing state.

## 6. Idempotency

The pipeline supports repeatable execution by:

- Using a transaction watermark.
- Maintaining cumulative account state.
- Writing snapshots to deterministic `snapshot_date` partitions.
- Dynamically overwriting the target partition.
- Updating the watermark only after successful completion.

A repeated run with no new transactions does not duplicate the previous result.

## 7. Data Quality

The Q1 pipeline performs validation including:

- Account row-count sanity check.
- Duplicate account detection.
- Transaction/account integrity check.
- Preservation of accounts with no transactions.
- Output validation before watermark update.

The dataset contains 7,954 accounts.

The initial snapshot successfully produced 7,954 account rows.

The transaction profiling identified:

- 1,832,050 completed transactions.
- 6,271 accounts with completed transactions.
- 1,683 accounts without completed transactions.
- 2,671 customers with multiple accounts.

### Balance Reconciliation Limitation

The supplied dataset contains the current account balance but does not provide a historical opening/previous balance for every account.

Therefore, exact historical balance reconciliation cannot be reconstructed solely from the supplied dataset.

In a production implementation, reconciliation should compare:

    Opening Balance
    + Credits
    - Debits
    = Closing Balance

against the authoritative account balance source.

## 8. Airflow

The Airflow DAG is:

    dags/account_snapshot_dag.py

DAG:

    account_snapshot_pipeline

Main dependency:

    account_snapshot
           |
           v
    trigger_credit_scoring

The DAG includes:

- Retry handling.
- Failure callback.
- Logging.
- Dependency management.
- Downstream credit-scoring trigger.

The current credit-scoring task is a placeholder integration point representing the production downstream Databricks/ML workflow.

## 9. Q1 Testing

Unit tests are located under:

    tests/

The test suite covers:

- Transformation logic.
- Incremental state.
- Aggregation behavior.

Run:

    pytest -q

# Q2 — PostgreSQL Analytics

## 10. Objective

Q2 implements:

1. Monthly Customer Health Scorecard.
2. Fraud Detection.
3. SQL optimization and incremental refresh design.

The implementation uses PostgreSQL.

Database:

    de_test_insignia

Schemas:

    bronze
    gold

## 11. Customer Health Scorecard

SQL file:

    q2_sql/customer_health_scorecard.sql

The scorecard uses CTEs and window functions to calculate:

- Total balance.
- Previous month balance.
- MoM balance change.
- MoM balance change percentage.
- Total transactions.
- Debit count.
- Credit count.
- Transfer in count.
- Transfer out count.
- Payment count.
- Fee count.
- Average transaction amount.
- Average transaction amount by channel.
- Credit utilization.
- Probability of default.
- Credit score.
- Model version.
- Risk flag.

### Risk Flag

A customer-month is flagged when:

    credit utilization > 80%
    OR
    probability of default > 0.3
    OR
    balance declined > 30% MoM

## 12. Balance Snapshot Integration

The Q1 account snapshot is integrated into PostgreSQL as:

    gold.account_balance_snapshot

This allows the Q2 scorecard to consume the account snapshot produced by Q1.

The snapshot is currently available for:

    2026-02-18

Because only one historical snapshot is available in the supplied dataset, historical MoM balance changes cannot be fully reconstructed for earlier months.

The architecture is designed to accumulate future snapshots over time.

## 13. Credit Score Handling

Credit scores are selected using an as-of-date approach:

    score_date <= score_month

This prevents future credit-score information from leaking into historical scorecard periods.

The supplied credit-score dataset ends earlier than the transaction dataset, so credit-score freshness is explicitly documented as a limitation.

## 14. Fraud Detection

SQL file:

    q2_sql/fraud_detection.sql

The implementation covers three assessment rules.

### Rule 1 : Transaction Velocity

Flag customers with:

    5 or more transactions within 1 hour

Implemented using a window function over customer transaction timestamps.

### Rule 2 : Multiple Cities

The assessment requires:

    3 or more cities within the same day

This requires merchant-location data.

The supplied transaction dataset does not contain merchant city information.

Therefore, the project defines:

    bronze.merchant_location

as a dependency table.

The table remains empty because merchant locations were not supplied.

No artificial location data is generated.

Therefore, Rule 2 is reported as unavailable due to missing source data rather than interpreted as zero fraud.

### Rule 3 : Large Transaction

Flag a transaction when:

    transaction amount > 3x
    30-day average transaction amount

The implementation uses the absolute transaction amount for the historical average because the dataset contains signed transaction amounts.

This is documented as a design assumption.

## 15. Fraud Results

The completed transaction population contains:

    1,832,050 completed transactions

Detection results from the supplied data:

    RULE_1_5_PLUS_TXNS_1H       29
    RULE_3_TXN_GT_3X_30D_AVG    1282

Rule 2 cannot be evaluated because merchant-location data is unavailable.

The generated detection output is:

    output/fraud_detections.txt

# Q2. SQL Optimization

## 16. Indexing

The optimization design includes indexes for:

- Completed transactions by account and transaction date.
- Customer/account relationships.
- Credit scores by customer and score date.
- Account balance snapshots by customer and snapshot date.

The transaction index uses a partial index:

    WHERE status = 'COMPLETED'

This reduces the index scope for the primary analytical workload.

## 17. EXPLAIN Validation

A broad transaction query was tested using PostgreSQL `EXPLAIN`.

The query used parallel execution and sequential scans for the broad workload.

A selective transaction query was able to use:

    idx_transactions_completed_account_date

and performed an index-only scan.

This demonstrates the difference between broad analytical scans and selective access patterns.

## 18. Incremental Refresh

The project defines:

    gold.scorecard_refresh_state

to maintain pipeline state for incremental scorecard refreshes.

Production refresh strategy:

1. Identify newly affected months.
2. Process only affected customer-month combinations.
3. Replace/upsert affected Gold records.
4. Commit the transaction.
5. Update the refresh-control state only after successful completion.

This avoids rebuilding the complete scorecard unnecessarily.

# Q3. Data Platform Architecture

## 19. Architecture Objective

The architecture supports:

- Q1 batch ELT.
- Q2 analytical workloads.
- Real time clickstream processing at 50K events/sec.
- Credit scoring.
- Fraud detection.
- Regulatory reporting.
- BI dashboards.
- Data governance.
- Column-level lineage.
- Hot/warm/cold storage.

The architecture follows a medallion-style design:

    Sources
       |
       v
    Ingestion
       |
       v
    Bronze
       |
       v
    Silver
       |
       v
    Gold
       |
       v
    Serving

## 20. Architecture Diagram

The detailed architecture diagram is provided separately:

    q3_architecture/a_wide_detailed_infographic_architecture_diagram.png

The architecture documentation is:

    q3_architecture/architecture.md

The diagram covers:

- Oracle Core Banking.
- Kafka / MSK.
- Zendesk.
- Braze / Google Ads.
- Credit scoring.
- Batch and streaming ingestion.
- S3 Bronze.
- Spark / PySpark.
- Silver and conformed data.
- Analytics.
- ML feature engineering.
- Fraud processing.
- Gold datasets.
- Regulatory reporting.
- BI dashboards.
- Risk analytics.
- Airflow orchestration.
- Governance and lineage.
- Hot/warm/cold storage.

## 21. Medallion Architecture

### Bronze

Raw immutable landing layer.

Technology:

- Amazon S3.
- AWS Glue Data Catalog.

### Silver

Cleaned and conformed data.

Technology:

- Spark / PySpark.
- Databricks or AWS Glue.
- Spark Structured Streaming.

Responsibilities:

- Standardization.
- Deduplication.
- Data quality.
- PII masking.
- Conformed customer/account identifiers.

### Gold

Business-ready datasets.

Examples:

    account_balance_snapshot
    customer_health_scorecard
    credit_score / probability_of_default
    fraud_detection
    customer analytics
    regulatory reporting

## 22. Real Time Architecture

The real time clickstream flow is:

    Mobile Application
           |
           v
    Kafka / Amazon MSK
           |
           v
    Spark Structured Streaming / Flink
           |
           v
    Silver
           |
           v
    Fraud / Analytics / Gold

The architecture uses:

- Kafka partitioning.
- Consumer groups.
- Event-time processing.
- Watermarking.
- Checkpointing.
- Dead-letter queues.
- Consumer-lag monitoring.

Streaming events are persisted to the data lake to support replay and reconciliation.

## 23. Batch and Streaming Integration

Streaming data uses common customer and account identifiers so that it can be joined with batch data.

Example:

    Kafka Event
         |
    customer_id / account_id
         |
         v
    Conformed Silver
         |
         +---- Batch Account Snapshot
         |
         +---- Transaction Data
         |
         v
    Gold Analytics / Fraud / Risk

This allows real time behavior to be analyzed together with historical banking data.

## 24. Data Governance

Governance is implemented across the platform.

Key controls include:

- IAM and least privilege.
- Encryption using AWS KMS.
- AWS Glue Data Catalog.
- Lake Formation.
- Data quality validation.
- Audit logging.
- PII masking.
- Column-level lineage.

Example lineage:

    Oracle.transactions.amount
            |
            v
    S3 Bronze
            |
            v
    Silver transactions.amount
            |
            v
    Gold.customer_health_scorecard.avg_transaction_amount
            |
            v
    BI Dashboard

OpenLineage-compatible instrumentation can be used with Airflow and Spark.

## 25. Hot / Warm / Cold Storage

### Hot

Recent, low latency data.

Examples:

- Recent clickstream.
- Real time fraud signals.

### Warm

Frequently accessed analytical data.

Examples:

- Recent transactions.
- Customer health scorecards.
- Account snapshots.

### Cold

Longterm historical data.

Potential technology:

    Amazon S3 Glacier

Lifecycle policies can automatically transition older data to lower-cost storage.

## 26. Cloud Budget

The proposed architecture targets approximately:

    $50,000/month

Illustrative allocation:

| Area | Budget |
|---|---:|
| Object storage / backup | $5,000 |
| Kafka / streaming | $10,000 |
| Spark / Databricks | $12,000 |
| Warehouse / serving | $10,000 |
| Orchestration / integration | $4,000 |
| Monitoring / governance / security | $5,000 |
| Contingency | $4,000 |
| **Total** | **$50,000** |

The allocation is illustrative and should be validated against actual workload characteristics and cloud pricing.

# 27. Project Structure

    DE-Test-Insignia/
    │
    ├── README.md
    ├── requirements.txt
    ├── .gitignore
    │
    ├── data/
    │   ├── customers.csv
    │   ├── accounts.csv
    │   ├── transactions.csv
    │   ├── credit_scores.csv
    │   ├── app_events.csv
    │   ├── support_tickets.csv
    │   └── acquisition_channels.csv
    │
    ├── q1_pipeline/
    │   ├── account_snapshot.py
    │   ├── config.py
    │   ├── profile_data.py
    │   ├── utils.py
    │   ├── q1_design.md
    │   └── __init__.py
    │
    ├── dags/
    │   └── account_snapshot_dag.py
    │
    ├── q2_sql/
    │   ├── customer_health_scorecard.sql
    │   ├── fraud_detection.sql
    │   └── optimization.sql
    │
    ├── q3_architecture/
    │   ├── architecture.md
    │   └── Q3 Platform Architecture New.png
    │   └── Q3 Platform Architecture New.drawio
    │
    ├── tests/
    │   ├── test_aggregation.py
    │   ├── test_incremental_state.py
    │   └── test_transformations.py
    │
    ├── docs/
    │   ├── data_profile.md
    │   ├── q1_design.md
    │   └── q2_explanation.md
    │
    └── output/
        ├── account_snapshots/
        ├── fraud_detections.txt
        └── pipeline_state.json

# 28. Technology Stack

| Area | Technology |
|---|---|
| ETL | PySpark |
| Orchestration | Apache Airflow |
| Analytics SQL | PostgreSQL |
| Testing | Pytest |
| Data Lake | Amazon S3 |
| Streaming | Kafka / Amazon MSK |
| Streaming Processing | Spark Structured Streaming / Flink |
| Batch Processing | Spark / Databricks / AWS Glue |
| Metadata | AWS Glue Data Catalog |
| Governance | Lake Formation / IAM |
| Encryption | AWS KMS |
| Serving | Redshift / Athena / BI |
| ML | Databricks ML / Credit Scoring Pipeline |
| Version Control | Git |

# 29. How to Run Q1

Activate the Python/Airflow environment:

    pyenv activate airflow-env

Run the PySpark pipeline:

    python q1_pipeline/account_snapshot.py

Run tests:

    pytest -q

Test the Airflow DAG:

    airflow dags test account_snapshot_pipeline 2026-09-30

The snapshot output is written under:

    output/account_snapshots/

# 30. Q2 PostgreSQL

Create the PostgreSQL database:

    de_test_insignia

Create the required schemas and tables, then load the supplied CSV datasets into the Bronze schema.

The analytical SQL files are located under:

    q2_sql/

Main files:

    customer_health_scorecard.sql
    fraud_detection.sql
    optimization.sql

# 31. Documentation

Additional design documentation:

    docs/data_profile.md
    docs/q1_design.md
    docs/q2_explanation.md
    q1_pipeline/q1_design.md
    q3_architecture/architecture.md

These documents provide additional implementation details, assumptions, limitations, and architectural reasoning.

# 32. Key Limitations and Assumptions

The following limitations are intentionally documented rather than hidden:

1. The supplied account balance is a current snapshot, so complete historical MoM balance reconciliation cannot be reconstructed from the provided data alone.

2. Merchant city/location data is not included in the transaction dataset. Therefore, the 3-city fraud rule requires an additional merchant-location source.

3. Credit score data ends earlier than the transaction period, so credit-score freshness must be monitored in production.

4. Credit utilization is calculated using customer total balance divided by customer total credit limit. This is a documented implementation assumption.

5. Fraud Rule 3 uses absolute transaction amounts because transaction amounts are represented as signed values in the supplied dataset.

6. The Q1 credit-scoring trigger is implemented as a downstream integration point. In production it would invoke the actual Databricks/ML workflow.

7. The Q3 architecture is a conceptual production architecture and does not provision actual AWS infrastructure.

# 33. Submission Notes

The project demonstrates the requested Data Engineering capabilities through:

- Production oriented incremental ETL.
- PySpark transformations.
- Airflow orchestration.
- Automated testing.
- PostgreSQL analytical SQL.
- CTEs and window functions.
- SQL indexing and query-plan analysis.
- Fraud detection logic.
- Incremental refresh design.
- Medallion architecture.
- Batch and streaming integration.
- Governance and lineage design.
- Explicit data limitations and assumptions.

The implementation prioritizes correctness, reproducibility, testability, observability, and clear architectural reasoning.
