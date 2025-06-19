-- Returns top 5 brands by receipts scanned for the most recent month as well as how 
-- those 5 brands compare to the previous months top 5 brand rankings.
-- Note: I chose to do the most recent FULL month for comparing, so recent month is actually the previous month
WITH current_month_rank AS (
    SELECT
        ib.name AS brand_name,
        DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT r.id) DESC) AS current_rank
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
SELECT
    COALESCE(rmr.brand_name, pmr.brand_name) AS brand_name,
    rmr.current_rank,
    pmr.previous_rank
FROM current_month_rank cmr
-- A FULL OUTER JOIN ensures the result includes brands that may have been in the top 5 in one month but not the other.
FULL OUTER JOIN previous_month_rank pmr ON cmr.brand_name = pmr.brand_name
WHERE
    rmr.current_rank <= 5 OR pmr.previous_rank <= 5
ORDER BY
    rmr.current_rank;


-- Returns average spend and total number of transactions for 'FINISHED' (Accepted) and 'REJECTED' receipts.
SELECT
    rewards_receipt_status,
    ROUND(AVG(total_spent), 2) AS average_spend,
    SUM(purchased_item_count) AS total_items_purchased
FROM
    receipt
WHERE
    rewards_receipt_status IN ('FINISHED', 'REJECTED')
GROUP BY
    rewards_receipt_status;


-- Returns brand with the most spend and most transactions amoung new users (created within past 6 months)
WITH new_user_brand_transactions AS (
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
brand_stats AS (
    SELECT
        brand_name,
        SUM(final_price) AS total_spend,
        COUNT(DISTINCT receipt_id) AS transaction_count
    FROM new_user_brand_transactions
    GROUP BY brand_name
)
(SELECT
    'Top Brand by Total Spend' AS metric,
    brand_name,
    ROUND(total_spend, 2) AS value
 FROM brand_stats
 ORDER BY total_spend DESC
 LIMIT 1)

UNION ALL

(SELECT
    'Top Brand by Transactions' AS metric,
    brand_name,
    transaction_count AS value
 FROM brand_stats
 ORDER BY transaction_count DESC
 LIMIT 1);