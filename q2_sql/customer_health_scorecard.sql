WITH monthly_transactions AS (
    SELECT
        a.customer_id,
        DATE_TRUNC('month', t.txn_date)::DATE AS score_month,
        COUNT(*) AS total_transactions,
        COUNT(*) FILTER (WHERE t.txn_type = 'DEBIT') AS debit_count,
        COUNT(*) FILTER (WHERE t.txn_type = 'CREDIT') AS credit_count,
        COUNT(*) FILTER (WHERE t.txn_type = 'TRANSFER_IN') AS transfer_in_count,
        COUNT(*) FILTER (WHERE t.txn_type = 'TRANSFER_OUT') AS transfer_out_count,
        COUNT(*) FILTER (WHERE t.txn_type = 'PAYMENT') AS payment_count,
        COUNT(*) FILTER (WHERE t.txn_type = 'FEE') AS fee_count,
        AVG(t.amount) AS avg_transaction_amount
    FROM bronze.transactions t
    JOIN bronze.accounts a
        ON t.account_id = a.account_id
    WHERE t.status = 'COMPLETED'
    GROUP BY
        a.customer_id,
        DATE_TRUNC('month', t.txn_date)::DATE
),

channel_avg AS (
    SELECT
        a.customer_id,
        DATE_TRUNC('month', t.txn_date)::DATE AS score_month,
        t.channel,
        ROUND(AVG(t.amount), 2) AS avg_amount
    FROM bronze.transactions t
    JOIN bronze.accounts a
        ON t.account_id = a.account_id
    WHERE t.status = 'COMPLETED'
    GROUP BY
        a.customer_id,
        DATE_TRUNC('month', t.txn_date)::DATE,
        t.channel
),

channel_metrics AS (
    SELECT
        customer_id,
        score_month,
        jsonb_object_agg(channel, avg_amount)
            AS avg_transaction_amount_by_channel
    FROM channel_avg
    GROUP BY
        customer_id,
        score_month
),

monthly_balance AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', snapshot_date)::DATE AS score_month,
        SUM(balance) AS total_balance,
        SUM(credit_limit) AS total_credit_limit
    FROM gold.account_balance_snapshot
    GROUP BY
        customer_id,
        DATE_TRUNC('month', snapshot_date)::DATE
),

balance_with_lag AS (
    SELECT
        customer_id,
        score_month,
        total_balance,
        total_credit_limit,
        LAG(total_balance) OVER (
            PARTITION BY customer_id
            ORDER BY score_month
        ) AS previous_month_balance
    FROM monthly_balance
),

customer_months AS (
    SELECT customer_id, score_month
    FROM monthly_transactions

    UNION

    SELECT customer_id, score_month
    FROM monthly_balance
),

credit_score_as_of_month AS (
    SELECT
        cm.customer_id,
        cm.score_month,
        cs.score_date,
        cs.model_version,
        cs.credit_score,
        cs.probability_of_default
    FROM customer_months cm
    LEFT JOIN LATERAL (
        SELECT
            score_date,
            model_version,
            credit_score,
            probability_of_default
        FROM bronze.credit_scores cs
        WHERE cs.customer_id = cm.customer_id
          AND cs.score_date <= cm.score_month
        ORDER BY
            cs.score_date DESC,
            cs.score_id DESC
        LIMIT 1
    ) cs
        ON TRUE
),

scorecard AS (
    SELECT
        cm.score_month,
        cm.customer_id,

        bwl.total_balance,
        bwl.previous_month_balance,

        bwl.total_balance
            - bwl.previous_month_balance
            AS mom_balance_change,

        CASE
            WHEN bwl.previous_month_balance IS NULL
              OR bwl.previous_month_balance = 0
            THEN NULL
            ELSE ROUND(
                (
                    (bwl.total_balance - bwl.previous_month_balance)
                    / ABS(bwl.previous_month_balance)
                ) * 100,
                4
            )
        END AS mom_balance_change_pct,

        COALESCE(mt.total_transactions, 0)
            AS total_transactions,

        COALESCE(mt.debit_count, 0)
            AS debit_count,

        COALESCE(mt.credit_count, 0)
            AS credit_count,

        COALESCE(mt.transfer_in_count, 0)
            AS transfer_in_count,

        COALESCE(mt.transfer_out_count, 0)
            AS transfer_out_count,

        COALESCE(mt.payment_count, 0)
            AS payment_count,

        COALESCE(mt.fee_count, 0)
            AS fee_count,

        mt.avg_transaction_amount,

        cmx.avg_transaction_amount_by_channel,

        CASE
            WHEN bwl.total_credit_limit IS NULL
              OR bwl.total_credit_limit = 0
            THEN NULL
            ELSE ROUND(
                (
                    bwl.total_balance
                    / bwl.total_credit_limit
                ) * 100,
                4
            )
        END AS credit_utilization_pct,

        csa.score_date AS credit_score_date,
        csa.model_version,
        csa.credit_score,
        csa.probability_of_default

    FROM customer_months cm

    LEFT JOIN monthly_transactions mt
        ON cm.customer_id = mt.customer_id
       AND cm.score_month = mt.score_month

    LEFT JOIN channel_metrics cmx
        ON cm.customer_id = cmx.customer_id
       AND cm.score_month = cmx.score_month

    LEFT JOIN balance_with_lag bwl
        ON cm.customer_id = bwl.customer_id
       AND cm.score_month = bwl.score_month

    LEFT JOIN credit_score_as_of_month csa
        ON cm.customer_id = csa.customer_id
       AND cm.score_month = csa.score_month
)

SELECT
    *,
    CASE
        WHEN credit_utilization_pct > 80
          OR probability_of_default > 0.3
          OR mom_balance_change_pct < -30
        THEN TRUE
        ELSE FALSE
    END AS risk_flag
FROM scorecard
ORDER BY
    score_month,
    customer_id;
