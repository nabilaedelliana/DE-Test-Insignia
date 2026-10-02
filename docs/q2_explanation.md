# Q2. PostgreSQL Customer Health Scorecard & Fraud Detection

## 1. Customer Health Scorecard

The Customer Health Scorecard is implemented in PostgreSQL using CTEs and window functions.

The transformation includes:

- Monthly transaction aggregation.
- Transaction counts by transaction type.
- Average transaction amount by channel.
- Monthly customer balance.
- Month-over-month balance change using LAG().
- Credit utilization.
- Credit score as-of the score month to avoid future data leakage.
- Risk flag based on the assessment thresholds.

Risk flag conditions:

- Credit utilization > 80%, OR
- Probability of default > 0.3, OR
- Balance declined > 30% month-over-month.

The scorecard is stored as a physical Gold table:

`gold.customer_health_scorecard`

The table currently contains:

- 25,966 customer-month rows.
- 4,000 distinct customers.
- Coverage from August 2025 through February 2026.
- 25,627 customer-months with transaction/channel metrics.
- 7,886 rows flagged as risk.

### Design assumptions and data limitations

The supplied dataset contains only one account balance snapshot for February 2026.

Therefore, historical monthly balance changes cannot be reconstructed reliably from the supplied data alone. The pipeline creates and stores historical account snapshots going forward so that future month over month calculations can be performed correctly.

Credit scores are selected using:

`score_date <= score_month`

This prevents a future credit score from being used for an earlier customer-month.

The latest available credit score in the supplied dataset is July 2025, while transaction data continues through February 2026. This freshness limitation is retained rather than fabricating newer scores.

Credit utilization is implemented as customer total balance divided by customer total credit limit. This is a documented design assumption because the assessment specifies the metric but does not prescribe the exact aggregation formula.

Channel-level metrics are stored as JSONB so that new transaction channels can be accommodated without adding new columns.

---

## 2. Fraud Detection

Three fraud rules from the assessment are implemented.

### Rule 1 : 5+ transactions within 1 hour

Implemented using a PostgreSQL window function with a one-hour `RANGE` frame.

Detected rows in the supplied dataset: 29

### Rule 2 : 3+ distinct cities in the same day

The transaction dataset does not contain merchant location.

A dependency table was therefore created:

`bronze.merchant_location`

The table contains:

- `txn_id`
- `merchant_city`

No merchant city data was fabricated.

Consequently, Rule 2 is implemented but currently produces no detections because the source location table is empty.

### Rule 3. Transaction greater than 3x 30-day average

Implemented using a trailing 30 day window.

The comparison uses transaction amount magnitude (`ABS(amount)`) because the transaction dataset contains both positive and negative signed amounts.

Detected rows: 1,282

The earliest transaction period does not have a complete 30-day lookback because the supplied transaction dataset starts on August 1, 2025. Therefore, early-period results use the history available in the dataset.

---

## 3. Query Optimization

The following indexes were created:

### Completed transaction access

```sql
CREATE INDEX idx_transactions_completed_account_date
ON bronze.transactions (account_id, txn_date)
INCLUDE (txn_id, amount, channel, txn_type)
WHERE status = 'COMPLETED';
```

This is a partial covering index focused on the completed transactions used by the analytical workload.

### Account lookup

```sql
CREATE INDEX idx_accounts_customer_account
ON bronze.accounts (customer_id, account_id);
```

### Credit score lookup

```sql
CREATE INDEX idx_credit_scores_customer_date
ON bronze.credit_scores (customer_id, score_date DESC)
INCLUDE (probability_of_default, credit_score, model_version);
```

This supports efficient retrieval of the latest available score for a customer as of a reporting month.

### Balance snapshot lookup

```sql
CREATE INDEX idx_balance_snapshot_customer_date
ON gold.account_balance_snapshot (customer_id, snapshot_date DESC)
INCLUDE (balance, credit_limit);
```

---

## 4. EXPLAIN ANALYZE Results

### Broad completed transaction aggregation

Query:

```sql
SELECT COUNT(*)
FROM bronze.transactions t
JOIN bronze.accounts a
    ON t.account_id = a.account_id
WHERE t.status = 'COMPLETED';
```

Observed execution:

- Execution time: approximately 731 ms.
- PostgreSQL used parallel execution.
- Two parallel workers were launched.
- The transaction table was accessed using a Parallel Sequential Scan.
- The join used a Hash Join.

The sequential scan is reasonable for this query because a large proportion of the transaction table is being processed. An index is not automatically faster when most rows must be read.

### Selective account transaction lookup

Query:

```sql
SELECT
    t.txn_id,
    t.txn_date,
    t.amount,
    t.channel
FROM bronze.transactions t
WHERE t.account_id = 'ACC0000001121'
  AND t.status = 'COMPLETED'
ORDER BY t.txn_date;
```

Observed execution:

```text
Index Only Scan using idx_transactions_completed_account_date
Rows returned: 260
Heap Fetches: 0
Execution Time: 2.835 ms
```

This demonstrates that the targeted partial covering index is effective for selective account-level access.

The `Index Only Scan` and zero heap fetches indicate that PostgreSQL was able to satisfy the query directly from the index without additional table-row reads.

---

## 5. Incremental Refresh Strategy

### Production choice: dbt incremental model

For the production implementation, the preferred approach is a **dbt incremental model**.

The three alternatives considered are:

| Option | Consideration |
|---|---|
| Snowflake Dynamic Tables | Good for automatically maintained derived tables, but introduces a Snowflake-specific implementation and is not aligned with the PostgreSQL implementation used for this assessment. |
| dbt Incremental Model | Well suited to transformation-heavy analytical models where only affected records need to be rebuilt. It also provides SQL-based transformations, testing, documentation, and lineage capabilities. |
| PostgreSQL Materialized View | Simple for local PostgreSQL workloads, but a standard materialized-view refresh generally requires refreshing the materialized result rather than selectively rebuilding affected customer-month records. |

**Selected approach: dbt incremental model.**

The customer health scorecard is transformation-heavy and its natural incremental grain is:

`customer_id + score_month`

Therefore, when new transactions, account snapshots, or credit-score data arrive, the production pipeline can identify affected customer-month keys and rebuild only those records.

The local PostgreSQL implementation demonstrates the same incremental processing pattern without requiring a dbt runtime.

A control table is maintained:

`gold.scorecard_refresh_state`

It stores:

- pipeline name
- last successful refresh timestamp
- last processed month
- update timestamp

The incremental refresh pattern is:

1. Identify newly arrived source data using ingestion watermarks.
2. Determine affected `customer_id + score_month` keys.
3. Recalculate only those affected customer-month records.
4. Merge the deterministic results into the Gold scorecard.
5. Update the refresh control state only after successful completion.
6. Commit the transaction.

This avoids rebuilding the entire historical scorecard for every incremental load.

The transformation is idempotent because rerunning the same affected customer-month keys produces the same deterministic Gold records.

### Why dbt incremental is appropriate

The scorecard contains CTEs, window functions, joins, aggregation, and business rules. These transformations are more naturally maintained as a version-controlled analytical model than as a simple materialized-view definition.

A production dbt implementation would conceptually use an incremental predicate based on the affected reporting period and customer keys. For example:

```sql
{{ config(
    materialized='incremental',
    unique_key=['score_month', 'customer_id']
) }}

-- transformation logic

{% if is_incremental() %}
WHERE score_month >= (
    SELECT MAX(score_month)
    FROM {{ this }}
)
{% endif %}
The exact incremental predicate should be expanded in production to account for late-arriving transactions and corrections. The local PostgreSQL implementation therefore uses an explicit affected-key refresh pattern rather than pretending that the local environment is running dbt.
## 6. Production Optimization Considerations

For production scale, additional optimization can include:

- Partitioning high-volume transaction data by transaction date.
- Partition pruning for monthly analytical workloads.
- Incremental processing based on transaction watermarks.
- Refreshing only affected customer-month keys.
- `EXPLAIN (ANALYZE, BUFFERS)` regression testing.
- Statistics maintenance using `ANALYZE`.
- Monitoring query execution time and buffer usage.
- Separating Bronze ingestion from Gold analytical workloads.
- Maintaining audit metadata for refresh start/end time and row counts.

The optimization strategy is workload-dependent. Large scans can benefit from parallel sequential scans, while selective lookups can benefit substantially from targeted covering indexes.

