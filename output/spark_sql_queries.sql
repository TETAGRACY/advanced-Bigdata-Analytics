-- SQL1_funnel_by_device_referrer
WITH s AS (
          SELECT session_id, device_type, referrer,
                 exists(page_views, p -> p.page_type = 'product_detail') AS viewed_product,
                 cart_items > 0                                          AS added_to_cart,
                 conversion_status = 'converted'                         AS converted
          FROM sessions)
        SELECT device_type, referrer,
               COUNT(*)                                             AS sessions,
               ROUND(100 * AVG(CAST(viewed_product AS INT)), 2)     AS pct_viewed_product,
               ROUND(100 * AVG(CAST(added_to_cart AS INT)), 2)      AS pct_added_to_cart,
               ROUND(100 * AVG(CAST(converted AS INT)), 2)          AS pct_converted,
               ROUND(100 * SUM(CAST(converted AS INT)) /
                     NULLIF(SUM(CAST(added_to_cart AS INT)), 0), 2) AS cart_to_order_pct
        FROM s
        GROUP BY device_type, referrer
        ORDER BY pct_converted DESC;

-- SQL2_top3_products_per_category
WITH prod_rev AS (
          SELECT category_name, product_id,
                 SUM(subtotal)                    AS revenue,
                 SUM(quantity)                    AS units,
                 SUM(subtotal * profit_margin)    AS gross_profit
          FROM transaction_items
          WHERE is_revenue
          GROUP BY category_name, product_id),
        ranked AS (
          SELECT *,
                 RANK() OVER (PARTITION BY category_name ORDER BY revenue DESC)   AS rnk,
                 revenue / SUM(revenue) OVER (PARTITION BY category_name)         AS share_of_category
          FROM prod_rev)
        SELECT r.category_name, r.rnk, r.product_id, p.name,
               ROUND(r.revenue, 2) AS revenue, r.units, ROUND(r.gross_profit, 2) AS gross_profit,
               ROUND(100 * r.share_of_category, 2) AS pct_of_category
        FROM ranked r JOIN products p USING (product_id)
        WHERE r.rnk <= 3
        ORDER BY r.category_name, r.rnk;

-- SQL3_daily_revenue_moving_avg
WITH daily AS (
          SELECT txn_date,
                 SUM(total)                     AS revenue,
                 COUNT(*)                       AS orders,
                 SUM(discount)                  AS discounts
          FROM transactions WHERE is_revenue
          GROUP BY txn_date)
        SELECT txn_date, ROUND(revenue, 2) AS revenue, orders, ROUND(discounts, 2) AS discounts,
               ROUND(revenue / orders, 2) AS aov,
               ROUND(AVG(revenue) OVER (ORDER BY txn_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)
                    AS revenue_7d_avg,
               txn_date BETWEEN DATE'2025-03-02' AND DATE'2025-03-08' AS promo_week,
               dayofweek(txn_date) IN (1, 7)  AS weekend
        FROM daily
        ORDER BY txn_date;

-- SQL4_view_vs_purchase_per_product
WITH v AS (SELECT product_id, COUNT(*) AS views, COUNT(DISTINCT session_id) AS view_sessions
                   FROM page_views WHERE page_type = 'product_detail' GROUP BY product_id),
             b AS (SELECT product_id, SUM(quantity) AS units, COUNT(DISTINCT transaction_id) AS orders,
                          SUM(subtotal) AS revenue
                   FROM transaction_items WHERE is_revenue GROUP BY product_id)
        SELECT p.product_id, p.name, p.category_name, p.base_price, p.is_active, p.current_stock,
               COALESCE(v.views, 0) AS views, COALESCE(b.units, 0) AS units_sold,
               COALESCE(b.orders, 0) AS orders, ROUND(COALESCE(b.revenue, 0), 2) AS revenue,
               ROUND(100 * COALESCE(b.orders, 0) / NULLIF(v.view_sessions, 0), 2) AS view_to_order_pct,
               DENSE_RANK() OVER (ORDER BY COALESCE(v.views, 0) DESC)  AS view_rank,
               DENSE_RANK() OVER (ORDER BY COALESCE(b.units, 0) DESC)  AS sales_rank
        FROM products p LEFT JOIN v USING (product_id) LEFT JOIN b USING (product_id)
        ORDER BY views DESC;

-- SQL5_mongo_users_join_hbase_sessions
-- mongo_users: {_id, geo_data:{...}, purchase_summary:{total_orders,total_spent,...}}
        -- hbase_user_sessions: row_key + 's:*' columns, exactly as stored in HBase
        WITH hb AS (
          SELECT split(row_key, '#')[0]                           AS user_id,
                 CAST(`s:duration_seconds` AS BIGINT)            AS duration,
                 `s:device_type`                                  AS device_type,
                 `s:conversion_status` = 'converted'              AS converted
          FROM hbase_user_sessions),
        engagement AS (
          SELECT user_id, COUNT(*) AS sessions, ROUND(AVG(duration)/60, 1) AS avg_session_min,
                 SUM(CAST(converted AS INT)) AS converted_sessions,
                 mode(device_type) AS main_device
          FROM hb GROUP BY user_id)
        SELECT CASE WHEN m.purchase_summary.total_orders = 0 THEN '0 orders'
                    WHEN m.purchase_summary.total_orders = 1 THEN '1 order'
                    WHEN m.purchase_summary.total_orders <= 3 THEN '2-3 orders'
                    ELSE '4+ orders' END                     AS buyer_segment,
               COUNT(*)                                      AS users,
               ROUND(AVG(e.sessions), 1)                     AS avg_sessions,
               ROUND(AVG(e.avg_session_min), 1)              AS avg_session_min,
               ROUND(AVG(m.purchase_summary.total_spent), 2) AS avg_spent,
               ROUND(SUM(m.purchase_summary.total_spent) / SUM(e.sessions), 2) AS revenue_per_session
        FROM mongo_users m JOIN engagement e ON m._id = e.user_id
        GROUP BY 1 ORDER BY 1;

-- SQL6_lost_demand_inactive_products
-- Demand that hits products which cannot be bought (inactive and/or out of stock):
        -- expected orders = view sessions x the median view->order rate of ACTIVE products.
        WITH v AS (SELECT product_id, COUNT(DISTINCT session_id) AS view_sessions
                   FROM page_views WHERE page_type = 'product_detail' GROUP BY product_id),
             o AS (SELECT product_id, COUNT(DISTINCT transaction_id) AS orders, AVG(unit_price) AS avg_price
                   FROM transaction_items WHERE is_revenue GROUP BY product_id),
             base AS (SELECT p.product_id, p.is_active, p.current_stock, p.base_price,
                             COALESCE(v.view_sessions, 0) AS view_sessions, COALESCE(o.orders, 0) AS orders
                      FROM products p LEFT JOIN v USING (product_id) LEFT JOIN o USING (product_id)),
             rate AS (SELECT percentile_approx(orders / view_sessions, 0.5) AS r
                      FROM base WHERE is_active AND view_sessions >= 50)
        SELECT CASE WHEN is_active THEN 'active'
                    WHEN current_stock = 0 THEN 'inactive - out of stock'
                    ELSE 'inactive - HAS stock (catalogue error?)' END   AS status,
               COUNT(*)                                                  AS products,
               SUM(view_sessions)                                        AS view_sessions,
               SUM(orders)                                               AS actual_orders,
               ROUND(SUM(view_sessions) * MAX(rate.r))                   AS expected_orders_if_buyable,
               ROUND(SUM(view_sessions * rate.r * base_price) - SUM(orders * base_price), 2)
                                                                         AS est_lost_revenue
        FROM base CROSS JOIN rate
        GROUP BY 1 ORDER BY 1;

