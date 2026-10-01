/*
Q2 - Query Optimization & Incremental Refresh

Optimization strategy:
1. Use targeted indexes on high-volume transaction and lookup paths.
2. Keep the customer health scorecard as a physical Gold table.
3. Refresh only affected score_month partitions instead of rebuilding
   the complete historical scorecard.
4. Track the last successful refresh in a control table.
5. Execute refresh in a transaction so a failed refresh does not leave
   the Gold table partially updated.

PostgreSQL note:
- A standard PostgreSQL materialized view performs a full refresh.
- Therefore, the assessment solution uses a physical Gold table with
  delete + insert/upsert for affected months.
*/

-- ============================================================
-- 1. Refresh control table
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.scorecard_refresh_state (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_successful_refresh TIMESTAMP,
    last_processed_month DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO gold.scorecard_refresh_state (
    pipeline_name,
    last_successful_refresh,
    last_processed_month
)
VALUES (
    'customer_health_scorecard',
    NULL,
    NULL
)
ON CONFLICT (pipeline_name) DO NOTHING;


-- ============================================================
-- 2. Supporting indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_transactions_completed_account_date
ON bronze.transactions (account_id, txn_date)
INCLUDE (txn_id, amount, channel, txn_type)
WHERE status = 'COMPLETED';

CREATE INDEX IF NOT EXISTS idx_accounts_customer_account
ON bronze.accounts (customer_id, account_id);

CREATE INDEX IF NOT EXISTS idx_credit_scores_customer_date
ON bronze.credit_scores (customer_id, score_date DESC)
INCLUDE (probability_of_default, credit_score, model_version);

CREATE INDEX IF NOT EXISTS idx_balance_snapshot_customer_date
ON gold.account_balance_snapshot (customer_id, snapshot_date DESC)
INCLUDE (balance, credit_limit);


-- ============================================================
-- 3. Incremental refresh pattern
-- ============================================================
--
-- Example:
--
-- BEGIN;
--
-- DELETE FROM gold.customer_health_scorecard
-- WHERE score_month BETWEEN DATE '2026-02-01'
--                       AND DATE '2026-02-01';
--
-- INSERT INTO gold.customer_health_scorecard (...)
-- SELECT ...
-- FROM gold.v_customer_health_scorecard
-- WHERE score_month BETWEEN DATE '2026-02-01'
--                       AND DATE '2026-02-01';
--
-- UPDATE gold.scorecard_refresh_state
-- SET last_successful_refresh = CURRENT_TIMESTAMP,
--     last_processed_month = DATE '2026-02-01',
--     updated_at = CURRENT_TIMESTAMP
-- WHERE pipeline_name = 'customer_health_scorecard';
--
-- COMMIT;
--
-- In production, the date range is derived from newly arrived data
-- and affected customer-month keys rather than being hard-coded.
--
-- The refresh is idempotent because rerunning the same month replaces
-- the existing Gold rows with the same deterministic transformation.


-- ============================================================
-- 4. Example incremental refresh state update
-- ============================================================
--
-- The actual INSERT statement is generated from
-- q2_sql/customer_health_scorecard.sql.
--
-- The following pattern demonstrates the control-table update
-- after a successful refresh:

-- UPDATE gold.scorecard_refresh_state
-- SET last_successful_refresh = CURRENT_TIMESTAMP,
--     last_processed_month = DATE '2026-02-01',
--     updated_at = CURRENT_TIMESTAMP
-- WHERE pipeline_name = 'customer_health_scorecard';
