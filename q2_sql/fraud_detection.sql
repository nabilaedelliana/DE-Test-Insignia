/*
Q2 - Fraud Detection

Rules from the assessment:
1. 5+ transactions within 1 hour.
2. 3+ distinct cities in the same day.
3. A single transaction greater than 3x the 30-day average.

Dataset limitation:
- bronze.transactions does not contain merchant location.
- Rule 2 therefore depends on bronze.merchant_location.
- The table is created as a source dependency, but no city data is fabricated.
- When merchant location data is available, Rule 2 becomes executable without changing
  the fraud detection logic.
*/

WITH transaction_windows AS (
    SELECT
        a.customer_id,
        t.txn_id,
        t.account_id,
        t.txn_date,
        t.amount,
        t.channel,
        COUNT(*) OVER (
            PARTITION BY a.customer_id
            ORDER BY t.txn_date
            RANGE BETWEEN
                INTERVAL '1 hour' PRECEDING
                AND CURRENT ROW
        ) AS transactions_last_1h
    FROM bronze.transactions t
    JOIN bronze.accounts a
        ON t.account_id = a.account_id
    WHERE t.status = 'COMPLETED'
),

rule_1 AS (
    SELECT
        customer_id,
        txn_id,
        account_id,
        txn_date,
        amount,
        channel,
        'RULE_1_5_PLUS_TXNS_1H' AS detection_rule,
        transactions_last_1h::NUMERIC AS rule_metric,
        'Transaction count within trailing 1-hour window' AS rule_description
    FROM transaction_windows
    WHERE transactions_last_1h >= 5
),

city_activity AS (
    SELECT
        a.customer_id,
        DATE_TRUNC('day', t.txn_date)::DATE AS txn_day,
        COUNT(DISTINCT ml.merchant_city) AS distinct_cities
    FROM bronze.transactions t
    JOIN bronze.accounts a
        ON t.account_id = a.account_id
    JOIN bronze.merchant_location ml
        ON t.txn_id = ml.txn_id
    WHERE t.status = 'COMPLETED'
    GROUP BY
        a.customer_id,
        DATE_TRUNC('day', t.txn_date)::DATE
),

rule_2_days AS (
    SELECT
        customer_id,
        txn_day,
        distinct_cities
    FROM city_activity
    WHERE distinct_cities >= 3
),

rule_2 AS (
    SELECT
        a.customer_id,
        t.txn_id,
        t.account_id,
        t.txn_date,
        t.amount,
        t.channel,
        'RULE_2_3_PLUS_CITIES_DAY' AS detection_rule,
        r2.distinct_cities::NUMERIC AS rule_metric,
        'Three or more distinct merchant cities in the same day' AS rule_description
    FROM rule_2_days r2
    JOIN bronze.accounts a
        ON a.customer_id = r2.customer_id
    JOIN bronze.transactions t
        ON t.account_id = a.account_id
       AND DATE_TRUNC('day', t.txn_date)::DATE = r2.txn_day
    WHERE t.status = 'COMPLETED'
),

transaction_history AS (
    SELECT
        a.customer_id,
        t.txn_id,
        t.account_id,
        t.txn_date,
        t.amount,
        t.channel,

        AVG(ABS(t.amount)) OVER (
            PARTITION BY a.customer_id
            ORDER BY t.txn_date
            RANGE BETWEEN
                INTERVAL '30 days' PRECEDING
                AND INTERVAL '1 microsecond' PRECEDING
        ) AS avg_amount_30d

    FROM bronze.transactions t
    JOIN bronze.accounts a
        ON t.account_id = a.account_id
    WHERE t.status = 'COMPLETED'
),

rule_3 AS (
    SELECT
        customer_id,
        txn_id,
        account_id,
        txn_date,
        amount,
        channel,
        'RULE_3_TXN_GT_3X_30D_AVG' AS detection_rule,
        ROUND(
            ABS(amount) / NULLIF(avg_amount_30d, 0),
            2
        ) AS rule_metric,
        'Transaction amount exceeds 3x trailing 30-day average magnitude' AS rule_description
    FROM transaction_history
    WHERE avg_amount_30d IS NOT NULL
      AND ABS(amount) > 3 * avg_amount_30d
),

all_detections AS (
    SELECT * FROM rule_1

    UNION ALL

    SELECT * FROM rule_2

    UNION ALL

    SELECT * FROM rule_3
)

SELECT
    customer_id,
    txn_id,
    account_id,
    txn_date,
    amount,
    channel,
    detection_rule,
    rule_metric,
    rule_description
FROM all_detections
ORDER BY
    txn_date,
    customer_id,
    txn_id,
    detection_rule;
