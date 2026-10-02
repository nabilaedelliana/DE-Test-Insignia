# Q3. Data Platform Architecture

## 1. Objective

The proposed data platform supports NegaraBank's batch analytics, regulatory reporting, Real time clickstream processing, credit scoring, fraud detection, governance, and column level lineage requirements.

The architecture is designed to support:

- Q1 batch account snapshot ETL.
- Q2 customer health scorecard and fraud detection.
- Real time mobile clickstream processing at up to 50,000 events/second.
- Credit scoring and ML feature engineering.
- Regulatory reporting and BI dashboards.
- PII protection and auditability.
- Column level data lineage from source systems to analytical outputs.
- Hot, warm, and cold data storage.
- A target cloud platform budget of approximately $50,000/month.

---

## 2. High level Architecture

The platform follows a medallion architecture consisting of Bronze, Silver, and Gold layers.

### Source Systems

1. Oracle Core Banking
   - Customer, account, and transaction data.
   - High volume transactional source.
   - Supports batch extraction and CDC.

2. Mobile Application Clickstream
   - Real time mobile events.
   - Peak throughput of approximately 50,000 events/second.
   - Ingested through Kafka.

3. Databricks Credit Scoring
   - Credit scoring and ML processing.
   - Produces credit score, probability of default (PD), model version, and related features.

4. Zendesk
   - Customer support tickets.
   - Ingested through API/connectors.

5. Braze
   - Customer acquisition and campaign data.
   - Ingested through APIs/connectors.

6. Historical Credit Score Files
   - `credit_scores.csv` represents batch credit-score data available from the assessment dataset.
   - Loaded through batch file ingestion.

---

## 3. Ingestion Layer

### Oracle Core Banking

AWS DMS is used for Oracle CDC/batch ingestion.

Flow:

Oracle Core Banking
→ AWS DMS
→ Amazon S3 Bronze

CDC reduces the need for repeated full-table extraction and allows the platform to process only changed records.

### Mobile Clickstream

Amazon MSK / Kafka is used for high throughput event ingestion.

Flow:

Mobile Application
→ Amazon MSK / Kafka
→ Spark Structured Streaming / Flink
→ S3 Bronze / Silver

Kafka provides buffering, partitioning, replay capability, and horizontal scalability.

### API / External Sources

Zendesk and Braze data are ingested through managed connectors or scheduled API extraction.

Flow:

External APIs
→ Ingestion Layer
→ S3 Bronze

### Batch File Ingestion

Historical CSV or other file-based datasets are loaded through scheduled batch ingestion.

Flow:

CSV / Batch Files
→ S3 Bronze
→ Spark / Databricks
→ Silver

---

## 4. Bronze Layer

The Bronze layer is the raw, immutable landing zone.

Technology:

- Amazon S3.
- AWS Glue Data Catalog.

Characteristics:

- Preserve source level records.
- Append-oriented where possible.
- Maintain ingestion timestamp.
- Maintain source system metadata.
- Preserve raw event payloads where required.
- Support replay and auditability.
- Partition high volume datasets by appropriate event/date attributes.

Example Bronze datasets:

- `bronze.customers`
- `bronze.accounts`
- `bronze.transactions`
- `bronze.credit_scores`
- `bronze.app_events`
- `bronze.support_tickets`
- `bronze.acquisition_channels`

The Bronze layer should not be directly consumed by business users.

---

## 5. Silver Layer

The Silver layer contains cleaned, standardized, deduplicated, and conformed data.

Primary technologies:

- Apache Spark / PySpark.
- Databricks or AWS Glue.
- Spark Structured Streaming for Real time workloads.

Typical transformations:

- Schema validation.
- Data type standardization.
- Deduplication.
- Null handling.
- Referential integrity checks.
- Timestamp normalization.
- Status normalization.
- PII masking.
- Data quality validation.
- Joining data from different source systems.
- Standardizing customer and account identifiers.

The Silver layer creates conformed entities that can be joined consistently across batch and streaming workloads.

### Silver Processing Branches

The Silver layer branches into three major processing paths:

1. Analytics Processing
   - Customer/account analytics.
   - Monthly aggregations.
   - Regulatory metrics.
   - Business KPIs.

2. ML Feature Engineering
   - Credit scoring features.
   - Customer behavior features.
   - Transaction aggregates.
   - Risk-related features.

3. Fraud Processing
   - Transaction velocity.
   - Transaction amount anomalies.
   - Geographic behavior.
   - Real time fraud signals.

---

## 6. Gold Layer

The Gold layer contains business-ready and regulatory-ready datasets.

Main datasets include:

### Account Balance Snapshot

The Q1 PySpark pipeline produces an incremental, conformed account snapshot in the Silver layer.

Example Silver dataset:

`silver.account_snapshot`

Partitioned by:

`snapshot_date`

The Silver account snapshot contains standardized account-level balances and cumulative transaction effects produced by the Q1 transformation.

The Q2 analytical layer then publishes the governed business-ready representation:

`gold.account_balance_snapshot`

This Gold dataset is used by the Customer Health Scorecard and regulatory analytics.

This separation keeps Q1 transformation output in Silver while reserving Gold for business-ready and regulatory-ready datasets.
### Customer Health Scorecard

Customer level monthly analytical view used by Q2.

Example:

`gold.customer_health_scorecard`

Contains metrics such as:

- Total balance.
- Previous month balance.
- MoM balance change.
- Transaction counts.
- Average transaction amount.
- Average transaction amount by channel.
- Credit utilization.
- Probability of default.
- Credit score.
- Model version.
- Risk flag.

### Credit Score / Probability of Default

Credit scoring outputs are maintained as governed Gold datasets and can be consumed by:

- Risk analytics.
- Customer health scorecards.
- Credit decisioning.
- Regulatory reporting.

### Fraud Detection

Fraud detection outputs contain rule-based or ML-based detections such as:

- 5+ transactions within one hour.
- Transactions exceeding 3x the 30-day average.
- Multiple cities within one day when merchant-location data is available.

### Customer Analytics

Customer level analytical datasets can combine:

- Customer attributes.
- Account information.
- Transactions.
- Credit scores.
- Support interactions.
- Acquisition information.
- Behavioral features.

### Regulatory Reporting

Regulatory datasets are generated from governed Gold tables with documented definitions and lineage.

---

## 7. Orchestration and Control Plane

Apache Airflow acts as the orchestration and control plane for batch workloads.

Example Q1 workflow:

Oracle / Source Data
→ Incremental Account Snapshot
→ Data Quality Validation
→ Write Gold Snapshot
→ Success
→ Trigger Credit Scoring

The Airflow DAG should provide:

- Scheduling.
- Dependency management.
- Retry handling.
- Failure callbacks.
- Logging.
- Monitoring.
- Backfill support.
- Idempotent task execution.

For the assessment implementation, the Q1 DAG uses:

`account_snapshot → trigger_credit_scoring`

The credit scoring task is represented as a downstream integration point and can be replaced by an actual Databricks or ML workflow trigger in production.

---

## 8. Batch Processing

Batch processing is primarily used for:

- Account snapshots.
- Monthly customer health scorecards.
- Regulatory reporting.
- Historical data processing.
- Credit score processing.
- External API ingestion.

Primary processing technologies:

- PySpark.
- Databricks.
- AWS Glue.
- SQL.
- Airflow.

The Q1 implementation uses an incremental watermark rather than repeatedly processing the full transaction history.

The pipeline stores the last successfully processed transaction timestamp and only processes new transactions during subsequent runs.

This supports:

- Incremental loading.
- Idempotency.
- Reduced processing cost.
- Reduced runtime.
- Backfill capability.

---

## 9. Real time Streaming

The Real time pipeline supports mobile clickstream processing at approximately 50,000 events/second.

Flow:

Mobile Application
→ Amazon MSK / Kafka
→ Spark Structured Streaming / Apache Flink
→ Silver Streaming Layer
→ Real time Analytics / Gold
→ Risk / Fraud / BI Consumers

Important streaming design considerations:

- Kafka partitions for horizontal scalability.
- Consumer groups.
- Event-time processing.
- Watermarking.
- Checkpointing.
- Exactly-once or effectively-once processing where supported.
- Dead-letter queue for malformed events.
- Schema validation.
- Consumer-lag monitoring.

Streaming data should also be persisted into the lake so that events can be replayed and reconciled with batch data.

---

## 10. Batch + Streaming Integration

Batch and streaming data should use common customer, account, transaction, and event identifiers.

This allows Real time clickstream data to be joined with historical account and customer information.

Example:

Mobile Event → customer_id → Silver Customer → Account / Transaction Data → Gold Customer Analytics

This enables use cases such as:

- Real time fraud detection.
- Customer behavioral analysis.
- Credit risk feature generation.
- Customer 360.
- Personalized customer experiences.

The architecture should avoid maintaining completely separate definitions of customer and account entities between batch and streaming systems.

---

## 11. Serving Layer

The Gold layer is exposed to different consumers according to their workload.

### Regulatory Reporting

Used for:

- OJK reporting.
- Auditable regulatory metrics.
- Compliance analysis.
- Historical reporting.

### BI Dashboards

Used by:

- Business teams.
- Management.
- Operations.
- Product teams.

Potential serving technology:

- Amazon Redshift.
- Athena.
- BI tools such as Power BI.

### Risk Analytics

Used by:

- Credit risk teams.
- Fraud teams.
- Risk models.
- ML pipelines.

Serving technology should be selected based on latency and workload requirements.

---

## 12. Hot, Warm, and Cold Data

### Hot Data

Used for recent and latency-sensitive data.

Examples:

- Recent clickstream events.
- Real time fraud signals.
- Operational risk metrics.

Potential technologies:

- Kafka/MSK.
- Real time processing engine.
- Low latency analytical store.

### Warm Data

Used for active analytics and reporting.

Examples:

- Recent transactions.
- Customer health scorecards.
- Account snapshots.
- BI datasets.

Potential technologies:

- S3.
- Redshift.
- Athena.

### Cold Data

Used for historical retention and regulatory/audit requirements.

Potential technology:

- Amazon S3 Glacier.

Data lifecycle policies can automatically transition older data from warm storage to cold storage to reduce storage costs.

---

## 13. Data Governance and Security

Governance is a cross-cutting capability across all layers.

### IAM

Use role based access control and least-privilege IAM policies.

Examples:

- Airflow service role.
- Spark/Databricks execution role.
- BI read-only role.
- Data engineering role.
- Data governance role.

### Encryption

Use encryption at rest and in transit.

Potential technology:

- AWS KMS.

### PII Protection

Sensitive customer information must be protected.

Examples:

- NIK should be masked.
- Phone numbers should expose only the last four digits.
- Sensitive attributes should not be unnecessarily exposed in Gold datasets.

### Data Catalog

AWS Glue Data Catalog provides metadata management and dataset discovery.

### Lake Formation

AWS Lake Formation can provide centralized access control and governance for data lake resources.

### Data Quality

Data quality checks should include:

- Row count sanity checks.
- Duplicate detection.
- Referential integrity.
- Null checks.
- Balance reconciliation.
- Credit-score anomaly checks.
- KYC validation.
- Transaction status validation.

Failed quality checks should prevent downstream publication where appropriate.

---

## 14. Column level Lineage

The platform should maintain column level lineage from source systems to final analytical outputs.

Example column-level lineage:

```text
Oracle.transactions.amount
        |
        v
Bronze.transactions.amount
        |
        v
Silver.transactions.amount
        |
        v
ABS(amount)
        |
        v
AVG(ABS(amount))
GROUP BY customer_id, score_month, channel
        |
        v
Gold.customer_health_scorecard.avg_transaction_amount_by_channel
        |
        v
BI Customer Health Dashboard


Transformation logic:

AVG(ABS(amount))
GROUP BY customer_id, score_month, channel


The lineage captures not only the source and target columns, but also the transformation applied between them.
For the Customer Health Scorecard, the source transaction amount is standardized through the Bronze and Silver layers, converted to transaction magnitude using ABS(amount), and aggregated by customer, reporting month, and transaction channel.
The resulting channel-level metrics are stored in:
gold.customer_health_scorecard.avg_transaction_amount_by_channel
This provides traceability from the original Oracle transaction amount to the customer-facing BI metric.

Lineage should also identify:

- Source system.
- Source table.
- Source column.
- Transformation.
- Target table.
- Target column.
- Downstream consumers.

OpenLineage compatible instrumentation can be used with Airflow and Spark to capture technical lineage metadata.

This supports the requirement for auditability of customer-facing metrics.

---

## 15. Q1 Integration

Q1 is implemented as an incremental PySpark account snapshot pipeline.

Flow:

Oracle / Transaction Source → Bronze → PySpark Incremental Transformation → Data Quality Checks → Silver Account Snapshot → Airflow Success → Credit Scoring Trigger


Key Q1 design principles:

- Incremental transaction processing.
- Watermark-based loading.
- Historical snapshot partitions.
- Idempotent execution.
- Row count validation.
- Duplicate checks.
- Orphan-account checks.
- Logging.
- Retry handling.
- Failure notification.
- Secret management.

The snapshot output is partitioned by:

`snapshot_date`

The production implementation should use managed secrets rather than credentials embedded in code or configuration files.

---

## 16. Q2 Integration

Q2 consumes conformed Silver data and the curated Gold account balance snapshot produced from the Q1 Silver snapshot.

Customer Health Scorecard flow:

Silver Transactions
+
Silver Customer / Account
+
Gold Account Balance Snapshot
+
Credit Score / PD
→
Gold Customer Health Scorecard

Fraud Detection flow:

Silver Transactions
+
Merchant Location Data
→
Fraud Detection Processing
→
Gold Fraud Detection

The Q2 PostgreSQL implementation demonstrates:

- CTEs.
- Window functions.
- Incremental refresh design.
- Indexing.
- EXPLAIN analysis.
- Fraud detection rules.
- Risk flag calculation.

---

## 17. Cost Allocation

The target platform budget is approximately $50,000/month.

### Budget Estimation Methodology

The $50,000/month budget is a high level planning estimate rather than an exact cloud bill. The assessment provides workload characteristics but does not provide enough infrastructure parameters, such as retention volume, cluster size, node count, concurrency, query frequency, or storage growth, to calculate an exact monthly cloud invoice.

The estimate was therefore built using a workload-driven allocation approach:

1. Identify the major workload drivers:
   - 2M+ core banking transactions per day
   - Up to 50K clickstream events/sec
   - Batch PySpark processing
   - Databricks-based credit scoring
   - Analytical and BI workloads
   - Regulatory governance, lineage, monitoring, and security

2. Map each workload driver to its primary cloud cost category:
   - High-volume clickstream → Kafka / streaming
   - Batch and ML processing → Spark / Databricks
   - Historical and raw data retention → object storage / backup
   - Analytical queries and BI → warehouse / serving
   - Pipeline scheduling and data integration → orchestration / integration
   - Auditability and compliance → monitoring / governance / security

3. Allocate the $50K monthly budget according to the expected relative consumption of each category, while reserving a contingency amount for workload variability and unexpected usage.

Illustrative allocation:

| Component | Monthly Budget |
|---|---:|
| Object storage / backup | $5,000 |
| Kafka / streaming | $10,000 |
| Spark / Databricks | $12,000 |
| Warehouse / serving | $10,000 |
| Orchestration / integration | $4,000 |
| Monitoring / governance / security | $5,000 |
| Contingency | $4,000 |
| **Total** | **$50,000** |

The allocation should be monitored continuously because streaming throughput and compute workloads can significantly affect monthly cloud spending.

---

## 18. Architecture Trade offs

### Lakehouse + Warehouse

S3 provides scalable and cost-efficient storage while a warehouse such as Redshift provides optimized SQL serving.

Trade-off:

- More components to operate.
- Better separation between storage and serving workloads.

### Batch + Streaming

Batch processing is appropriate for periodic analytical workloads while streaming is required for low latency clickstream and fraud use cases.

Trade-off:

- Higher operational complexity.
- Supports both historical analytics and Real time use cases.

### Managed Services

Managed AWS services reduce infrastructure-management overhead.

Trade-off:

- Potentially higher service costs.
- Greater dependency on cloud-specific services.

### Hot / Warm / Cold Storage

Keeping all data in hot storage increases cost.

Lifecycle management allows frequently accessed data to remain readily available while older data is moved to lower-cost storage.

---

## 19. Production Considerations

A production implementation should additionally include:

- Infrastructure as Code.
- CI/CD.
- Automated unit and integration testing.
- Data contract enforcement.
- Schema registry for streaming events.
- Centralized logging.
- Metrics and alerting.
- SLA/SLO monitoring.
- Cost monitoring.
- Disaster recovery.
- Backup and restore procedures.
- Data retention policies.
- Access reviews.
- Security auditing.

---

## 20. Summary

The proposed architecture uses a cloud-based medallion data platform with:

- Oracle CDC/batch ingestion.
- Kafka/MSK for Real time clickstream.
- S3 and Glue Catalog for the data lake.
- Spark/PySpark for batch and streaming transformation.
- Databricks/Glue for scalable processing and ML integration.
- Airflow as the batch orchestration and control plane.
- Gold datasets for account snapshots, customer health, credit scoring, fraud detection, customer analytics, and regulatory reporting.
- Redshift/Athena and BI tools for serving.
- IAM, KMS, Lake Formation, data quality, and lineage for governance.

The architecture integrates the assessment's Q1 incremental ETL pipeline and Q2 analytical workloads into a single platform while providing a scalable path for real-time processing, credit scoring, fraud detection, regulatory reporting, and governed BI.
