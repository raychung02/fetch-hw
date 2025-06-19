-- CTE to get top 5 brands by receipts scanned for the most recent month as well as how those 5 brands compare to the previous months top 5 brand rankings.
-- Note: I chose to do the most recent FULL month for comparing, so recent month is actually the previous month
WITH recent_month_rank AS (
    SELECT
        ib.name AS brand_name,
        DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT r.id) DESC) AS recent_rank
    FROM receipt r
    JOIN receipt_item i ON r.id = i.receipt_id
    JOIN item_brand ib ON i.brand_code = ib.brand_code
    WHERE
        MONTH(r.date_scanned) = MONTH(now()) - 1
        AND i.brand_code IS NOT NULL
    GROUP BY b.name
),
previous_month_rank AS (
    SELECT
        ib.name AS brand_name,
        DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT r.id) DESC) AS previous_rank
    FROM receipt r
    JOIN receipt_item i ON r.id = i.receipt_id
    JOIN item_brand b ON i.brand_code = b.brand_code
    WHERE
        MONTH(r.date_scanned) = MONTH(now()) - 2
        AND i.brand_code IS NOT NULL
    GROUP BY b.name
)
brand_rank_metrics AS (
    SELECT
        '1. Top 5 Brands by Receipts Scanned' AS metric,
        COALESCE(rmr.brand_name, pmr.brand_name) AS subject,
        CONCAT(
            'Recent Month Rank: ', COALESCE(rmr.recent_rank, 'N/A'),
            ', Previous Month Rank: ', COALESCE(pmr.previous_rank, 'N/A')
        ) AS result
    FROM recent_month_rank rmr
    -- A FULL OUTER JOIN ensures the result includes brands that may have been in the top 5 in one month but not the other.
    FULL OUTER JOIN previous_month_rank pmr ON rmr.brand_name = pmr.brand_name
    WHERE rmr.recent_rank <= 5 OR pmr.previous_rank <= 5
),
-- CTE to get average spend and total number of transactions for 'FINISHED' (Accepted) and 'REJECTED' receipts.
receipt_status_metrics AS (
    SELECT
        '2. Receipt Status Metrics' AS metric,
        rewards_receipt_status AS subject,
        CONCAT(
            'Avg Spend: $', ROUND(AVG(total_spent), 2),
            ', Total Items Purchased: ', SUM(purchased_item_count)
        ) AS result
    FROM receipt
    WHERE rewards_receipt_status IN ('FINISHED', 'REJECTED')
    GROUP BY rewards_receipt_status
),
-- CTE to get top brands based on new user spend/transactions
new_user_brand_transactions AS (
    SELECT
        ib.name AS brand_name,
        i.final_price,
        r.id AS receipt_id
    FROM receipt r
    JOIN user u ON r.user_id = u.id
    JOIN receipt_item i ON r.id = i.receipt_id
    JOIN item_brand ib ON i.brand_code = b.brand_code
    WHERE
        u.created_date >= DATE_SUB(now(), INTERVAL 6 MONTH)
        AND i.final_price IS NOT NULL
        AND i.brand_code IS NOT NULL
),
top_brand_new_user_metrics AS (
    (SELECT '3. Top Brand by New User Spend' AS metric, brand_name AS subject, CAST(ROUND(total_spend, 2) AS CHAR) AS result FROM new_user_brand_transactions ORDER BY total_spend DESC LIMIT 1)
    UNION ALL
    (SELECT '4. Top Brand by New User Transactions' AS metric, brand_name AS subject, CAST(transaction_count AS CHAR) AS result FROM new_user_brand_transactions ORDER BY transaction_count DESC LIMIT 1)
)
SELECT metric, subject, result FROM brand_rank_metrics
UNION ALL
SELECT metric, subject, result FROM receipt_status_metrics
UNION ALL
SELECT metric, subject, result FROM top_brand_new_user_metrics
ORDER BY metric, result DESC;