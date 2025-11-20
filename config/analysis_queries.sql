-- ============================================
-- CÁC CÂU TRUY VẤN PHÂN TÍCH - QUERY TRỰC TIẾP TỪ BẢNG
-- ============================================
-- Tất cả queries này query trực tiếp từ các bảng:
-- - all_products: Dữ liệu sản phẩm hiện tại
-- - price_history: Lịch sử giá, số lượng bán, đánh giá theo ngày
-- - sale_periods: Các mùa sale

-- ============================================
-- 1. ẢNH HƯỞNG CỦA GIÁ CẢ ĐẾN SỐ LƯỢNG BÁN
-- ============================================

-- Câu hỏi 1.1: Khi giá sản phẩm tăng, số lượng bán có giảm không?
-- Phân tích trend giá và số lượng bán theo thời gian
WITH price_trends AS (
    SELECT 
        ph.name,
        ph.source,
        ph.category,
        ph.brand,
        ph.crawl_date,
        ph.price,
        ph.sold_count,
        LAG(ph.price) OVER (PARTITION BY ph.name, ph.source ORDER BY ph.crawl_date) AS prev_price,
        LAG(ph.sold_count) OVER (PARTITION BY ph.name, ph.source ORDER BY ph.crawl_date) AS prev_sold_count
    FROM price_history ph
    WHERE ph.price > 0 AND ph.sold_count >= 0
)
SELECT 
    name,
    source,
    category,
    crawl_date,
    price,
    sold_count,
    prev_price,
    prev_sold_count,
    CASE 
        WHEN prev_price IS NOT NULL THEN price - prev_price
        ELSE NULL 
    END AS price_change,
    CASE 
        WHEN prev_sold_count IS NOT NULL THEN sold_count - prev_sold_count
        ELSE NULL 
    END AS sold_change,
    CASE 
        WHEN prev_price IS NOT NULL AND prev_sold_count IS NOT NULL THEN
            CASE 
                WHEN (price - prev_price) > 0 AND (sold_count - prev_sold_count) < 0 THEN 'Giá tăng, bán giảm'
                WHEN (price - prev_price) > 0 AND (sold_count - prev_sold_count) > 0 THEN 'Giá tăng, bán tăng'
                WHEN (price - prev_price) < 0 AND (sold_count - prev_sold_count) > 0 THEN 'Giá giảm, bán tăng'
                WHEN (price - prev_price) < 0 AND (sold_count - prev_sold_count) < 0 THEN 'Giá giảm, bán giảm'
                ELSE 'Không thay đổi'
            END
        ELSE NULL
    END AS trend_analysis
FROM price_trends
WHERE prev_price IS NOT NULL AND prev_sold_count IS NOT NULL
ORDER BY crawl_date DESC, ABS(price - prev_price) DESC
LIMIT 100;

-- Tổng hợp correlation giữa giá và số lượng bán
WITH price_trends AS (
    SELECT 
        ph.category,
        ph.source,
        ph.price,
        ph.sold_count,
        LAG(ph.price) OVER (PARTITION BY ph.name, ph.source ORDER BY ph.crawl_date) AS prev_price,
        LAG(ph.sold_count) OVER (PARTITION BY ph.name, ph.source ORDER BY ph.crawl_date) AS prev_sold_count
    FROM price_history ph
    WHERE ph.price > 0 AND ph.sold_count >= 0
)
SELECT 
    category,
    source,
    COUNT(*) AS observations,
    ROUND(AVG(CASE WHEN (price - prev_price) > 0 AND (sold_count - prev_sold_count) < 0 THEN 1.0 ELSE 0.0 END)::numeric, 3) AS price_up_sales_down_ratio,
    ROUND(AVG(CASE WHEN (price - prev_price) < 0 AND (sold_count - prev_sold_count) > 0 THEN 1.0 ELSE 0.0 END)::numeric, 3) AS price_down_sales_up_ratio,
    ROUND(CORR((price - prev_price), (sold_count - prev_sold_count))::numeric, 3) AS price_sales_correlation
FROM price_trends
WHERE prev_price IS NOT NULL AND prev_sold_count IS NOT NULL
GROUP BY category, source
ORDER BY price_sales_correlation;

-- Câu hỏi 1.2: Sản phẩm giá cao có bán chạy không?
-- So sánh số lượng bán giữa các nhóm giá
SELECT 
    category,
    source,
    CASE 
        WHEN price < 5000000 THEN 'Giá thấp (<5M)'
        WHEN price >= 5000000 AND price < 15000000 THEN 'Giá trung bình (5M-15M)'
        WHEN price >= 15000000 AND price < 30000000 THEN 'Giá cao (15M-30M)'
        ELSE 'Giá rất cao (>30M)'
    END AS price_range,
    COUNT(*) AS product_count,
    ROUND(AVG(sold_count)::numeric, 2) AS avg_sold_count,
    ROUND(AVG(review_count)::numeric, 2) AS avg_review_count,
    ROUND(AVG(review_score)::numeric, 2) AS avg_review_score,
    ROUND(AVG(price)::numeric, 0) AS avg_price,
    ROUND(MAX(sold_count)::numeric, 0) AS max_sold_count,
    ROUND(MIN(sold_count)::numeric, 0) AS min_sold_count
FROM all_products
WHERE price > 0 AND sold_count >= 0
GROUP BY category, source, 
    CASE 
        WHEN price < 5000000 THEN 'Giá thấp (<5M)'
        WHEN price >= 5000000 AND price < 15000000 THEN 'Giá trung bình (5M-15M)'
        WHEN price >= 15000000 AND price < 30000000 THEN 'Giá cao (15M-30M)'
        ELSE 'Giá rất cao (>30M)'
    END
ORDER BY category, source, 
    CASE 
        WHEN price < 5000000 THEN 1
        WHEN price >= 5000000 AND price < 15000000 THEN 2
        WHEN price >= 15000000 AND price < 30000000 THEN 3
        ELSE 4
    END;

-- ============================================
-- 2. MỐI QUAN HỆ GIỮA ĐÁNH GIÁ VÀ SỐ LƯỢNG BÁN
-- ============================================

-- Câu hỏi 2.1: Sản phẩm giá cao có nhận được nhiều đánh giá tích cực hơn không?
SELECT 
    category,
    source,
    CASE 
        WHEN price < 5000000 THEN 'Giá thấp'
        WHEN price >= 5000000 AND price < 15000000 THEN 'Giá trung bình'
        WHEN price >= 15000000 AND price < 30000000 THEN 'Giá cao'
        ELSE 'Giá rất cao'
    END AS price_segment,
    COUNT(*) AS product_count,
    ROUND(AVG(price)::numeric, 0) AS avg_price,
    ROUND(AVG(review_score)::numeric, 2) AS avg_review_score,
    ROUND(AVG(review_count)::numeric, 2) AS avg_review_count,
    ROUND(SUM(CASE WHEN review_score >= 4.0 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS high_rating_ratio_pct,
    ROUND(SUM(CASE WHEN review_score >= 4.5 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS very_high_rating_ratio_pct
FROM all_products
WHERE price > 0 AND review_count > 0
GROUP BY category, source,
    CASE 
        WHEN price < 5000000 THEN 'Giá thấp'
        WHEN price >= 5000000 AND price < 15000000 THEN 'Giá trung bình'
        WHEN price >= 15000000 AND price < 30000000 THEN 'Giá cao'
        ELSE 'Giá rất cao'
    END
ORDER BY category, source,
    CASE 
        WHEN price < 5000000 THEN 1
        WHEN price >= 5000000 AND price < 15000000 THEN 2
        WHEN price >= 15000000 AND price < 30000000 THEN 3
        ELSE 4
    END;

-- Câu hỏi 2.2: Tăng số lượng đánh giá có làm tăng số lượng bán không?
-- Phân tích theo segment số lượng đánh giá
SELECT 
    category,
    source,
    CASE 
        WHEN review_count = 0 THEN 'Không có đánh giá'
        WHEN review_count < 10 THEN 'Ít đánh giá (1-9)'
        WHEN review_count < 100 THEN 'Trung bình (10-99)'
        WHEN review_count < 500 THEN 'Nhiều đánh giá (100-499)'
        ELSE 'Rất nhiều đánh giá (500+)'
    END AS review_segment,
    COUNT(*) AS product_count,
    ROUND(AVG(sold_count)::numeric, 2) AS avg_sold_count,
    ROUND(AVG(review_count)::numeric, 2) AS avg_review_count,
    ROUND(AVG(review_score)::numeric, 2) AS avg_review_score,
    ROUND(AVG(price)::numeric, 0) AS avg_price,
    ROUND(MEDIAN(sold_count)::numeric, 2) AS median_sold_count
FROM all_products
WHERE sold_count >= 0
GROUP BY category, source,
    CASE 
        WHEN review_count = 0 THEN 'Không có đánh giá'
        WHEN review_count < 10 THEN 'Ít đánh giá (1-9)'
        WHEN review_count < 100 THEN 'Trung bình (10-99)'
        WHEN review_count < 500 THEN 'Nhiều đánh giá (100-499)'
        ELSE 'Rất nhiều đánh giá (500+)'
    END
ORDER BY category, source,
    CASE 
        WHEN review_count = 0 THEN 1
        WHEN review_count < 10 THEN 2
        WHEN review_count < 100 THEN 3
        WHEN review_count < 500 THEN 4
        ELSE 5
    END;

-- Correlation giữa review_count và sold_count
SELECT 
    category,
    source,
    ROUND(CORR(review_count, sold_count)::numeric, 3) AS review_sales_correlation,
    COUNT(*) AS product_count,
    ROUND(AVG(review_count)::numeric, 2) AS avg_review_count,
    ROUND(AVG(sold_count)::numeric, 2) AS avg_sold_count
FROM all_products
WHERE review_count > 0 AND sold_count > 0
GROUP BY category, source
ORDER BY review_sales_correlation DESC;

-- Phân tích trend: Tăng review_count theo thời gian có làm tăng sold_count?
WITH review_trends AS (
    SELECT 
        ph.name,
        ph.source,
        ph.category,
        ph.crawl_date,
        ph.review_count,
        ph.sold_count,
        LAG(ph.review_count) OVER (PARTITION BY ph.name, ph.source ORDER BY ph.crawl_date) AS prev_review_count,
        LAG(ph.sold_count) OVER (PARTITION BY ph.name, ph.source ORDER BY ph.crawl_date) AS prev_sold_count
    FROM price_history ph
    WHERE ph.review_count >= 0 AND ph.sold_count >= 0
)
SELECT 
    category,
    source,
    COUNT(*) AS observations,
    ROUND(AVG(CASE WHEN (review_count - prev_review_count) > 0 AND (sold_count - prev_sold_count) > 0 THEN 1.0 ELSE 0.0 END)::numeric, 3) AS review_up_sales_up_ratio,
    ROUND(CORR((review_count - prev_review_count), (sold_count - prev_sold_count))::numeric, 3) AS review_sales_trend_correlation
FROM review_trends
WHERE prev_review_count IS NOT NULL AND prev_sold_count IS NOT NULL
GROUP BY category, source
ORDER BY review_sales_trend_correlation DESC;

-- Câu hỏi 2.3: Sản phẩm nào có số lượng đánh giá nhiều nhất nhưng lại bán ít nhất?
SELECT 
    name,
    source,
    category,
    brand,
    price,
    sold_count,
    review_count,
    ROUND(review_score::numeric, 2) AS review_score,
    CASE 
        WHEN sold_count > 0 THEN ROUND((review_count::FLOAT / sold_count)::numeric, 2)
        ELSE NULL 
    END AS reviews_per_sale,
    crawled_at
FROM all_products
WHERE review_count > 50 AND sold_count < 100
ORDER BY review_count DESC, sold_count ASC
LIMIT 50;

-- ============================================
-- 3. THỜI ĐIỂM BÁN HÀNG
-- ============================================

-- Câu hỏi 3.1: Sản phẩm bán tốt nhất vào thời điểm nào trong năm?
SELECT 
    sp.period_name,
    sp.period_type,
    ph.category,
    ph.source,
    COUNT(DISTINCT ph.name) AS unique_products,
    ROUND(AVG(ph.price)::numeric, 0) AS avg_price,
    ROUND(AVG(ph.sold_count)::numeric, 2) AS avg_sold_count,
    ROUND(AVG(ph.review_count)::numeric, 2) AS avg_review_count,
    ROUND(AVG(ph.review_score)::numeric, 2) AS avg_review_score,
    SUM(ph.sold_count) AS total_sold
FROM price_history ph
JOIN sale_periods sp ON ph.crawl_date BETWEEN sp.start_date AND sp.end_date
GROUP BY sp.period_name, sp.period_type, ph.category, ph.source
ORDER BY total_sold DESC, period_type;

-- So sánh tổng số lượng bán theo từng mùa sale
SELECT 
    sp.period_type,
    SUM(ph.sold_count) AS total_sold_all_categories,
    ROUND(AVG(ph.sold_count)::numeric, 2) AS avg_sold_per_product,
    COUNT(DISTINCT ph.category) AS categories_count,
    COUNT(DISTINCT ph.name) AS total_unique_products
FROM price_history ph
JOIN sale_periods sp ON ph.crawl_date BETWEEN sp.start_date AND sp.end_date
GROUP BY sp.period_type
ORDER BY total_sold_all_categories DESC;

-- Câu hỏi 3.2: Mức độ thay đổi giá trong các mùa sale
-- So sánh giá trong sale với giá bình thường
WITH period_prices AS (
    SELECT 
        ph.name,
        ph.source,
        ph.category,
        sp.period_type,
        AVG(ph.price) AS avg_price_in_period
    FROM price_history ph
    JOIN sale_periods sp ON ph.crawl_date BETWEEN sp.start_date AND sp.end_date
    GROUP BY ph.name, ph.source, ph.category, sp.period_type
),
normal_prices AS (
    SELECT 
        ph.name,
        ph.source,
        ph.category,
        AVG(ph.price) AS avg_price_normal
    FROM price_history ph
    JOIN sale_periods sp ON ph.crawl_date BETWEEN sp.start_date AND sp.end_date
    WHERE sp.period_type = 'Normal'
    GROUP BY ph.name, ph.source, ph.category
)
SELECT 
    pp.period_type,
    pp.category,
    pp.source,
    COUNT(DISTINCT pp.name) AS product_count,
    ROUND(AVG(pp.avg_price_in_period)::numeric, 0) AS avg_price_in_period,
    ROUND(AVG(np.avg_price_normal)::numeric, 0) AS avg_price_normal,
    ROUND(AVG((pp.avg_price_in_period - np.avg_price_normal) / NULLIF(np.avg_price_normal, 0) * 100)::numeric, 2) AS avg_price_change_percent,
    ROUND(MIN((pp.avg_price_in_period - np.avg_price_normal) / NULLIF(np.avg_price_normal, 0) * 100)::numeric, 2) AS min_price_change_percent,
    ROUND(MAX((pp.avg_price_in_period - np.avg_price_normal) / NULLIF(np.avg_price_normal, 0) * 100)::numeric, 2) AS max_price_change_percent
FROM period_prices pp
LEFT JOIN normal_prices np ON pp.name = np.name AND pp.source = np.source AND pp.category = np.category
WHERE pp.period_type != 'Normal' AND np.avg_price_normal IS NOT NULL
GROUP BY pp.period_type, pp.category, pp.source
ORDER BY pp.period_type, avg_price_change_percent;

-- ============================================
-- 4. XU HƯỚNG CHUNG
-- ============================================

-- Câu hỏi 4.1: Có xu hướng chung về sự thay đổi giá và số lượng bán trong cùng một loại hàng hóa không?
-- Xu hướng theo category theo thời gian
SELECT 
    category,
    source,
    crawl_date,
    COUNT(DISTINCT name) AS product_count,
    ROUND(AVG(price)::numeric, 0) AS avg_price,
    ROUND(AVG(sold_count)::numeric, 2) AS avg_sold_count,
    ROUND(AVG(review_count)::numeric, 2) AS avg_review_count,
    ROUND(AVG(review_score)::numeric, 2) AS avg_review_score,
    SUM(sold_count) AS total_sold
FROM price_history
WHERE crawl_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY category, source, crawl_date
ORDER BY category, source, crawl_date;

-- Tính toán trend (tăng/giảm) theo category
WITH category_trends AS (
    SELECT 
        category,
        source,
        crawl_date,
        AVG(price) AS avg_price,
        AVG(sold_count) AS avg_sold_count
    FROM price_history
    GROUP BY category, source, crawl_date
),
category_trends_lag AS (
    SELECT 
        category,
        source,
        crawl_date,
        avg_price,
        avg_sold_count,
        LAG(avg_price) OVER (PARTITION BY category, source ORDER BY crawl_date) AS prev_avg_price,
        LAG(avg_sold_count) OVER (PARTITION BY category, source ORDER BY crawl_date) AS prev_avg_sold_count
    FROM category_trends
)
SELECT 
    category,
    source,
    crawl_date,
    ROUND(avg_price::numeric, 0) AS avg_price,
    ROUND(avg_sold_count::numeric, 2) AS avg_sold_count,
    CASE 
        WHEN prev_avg_price IS NOT NULL 
        THEN ROUND(((avg_price - prev_avg_price) / NULLIF(prev_avg_price, 0) * 100)::numeric, 2)
        ELSE NULL 
    END AS price_change_percent,
    CASE 
        WHEN prev_avg_sold_count IS NOT NULL 
        THEN ROUND(((avg_sold_count - prev_avg_sold_count) / NULLIF(prev_avg_sold_count, 0) * 100)::numeric, 2)
        ELSE NULL 
    END AS sold_change_percent
FROM category_trends_lag
WHERE prev_avg_price IS NOT NULL
ORDER BY category, source, crawl_date DESC;

-- Câu hỏi 4.2: Yếu tố nào (giá, đánh giá, số lượng bán) có ảnh hưởng lớn nhất đến sự thành công của sản phẩm?
-- Phân tích theo mức độ thành công
SELECT 
    category,
    source,
    CASE 
        WHEN sold_count >= 1000 THEN 'Bán rất tốt'
        WHEN sold_count >= 500 THEN 'Bán tốt'
        WHEN sold_count >= 100 THEN 'Bán trung bình'
        WHEN sold_count >= 10 THEN 'Bán ít'
        ELSE 'Bán rất ít'
    END AS sales_performance,
    COUNT(*) AS product_count,
    ROUND(AVG(price)::numeric, 0) AS avg_price,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price))::numeric, 0) AS median_price,
    ROUND(AVG(review_count)::numeric, 2) AS avg_review_count,
    ROUND(AVG(review_score)::numeric, 2) AS avg_review_score,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_score))::numeric, 2) AS median_review_score,
    ROUND(AVG(sold_count)::numeric, 2) AS avg_sold_count
FROM all_products
WHERE price > 0
GROUP BY category, source,
    CASE 
        WHEN sold_count >= 1000 THEN 'Bán rất tốt'
        WHEN sold_count >= 500 THEN 'Bán tốt'
        WHEN sold_count >= 100 THEN 'Bán trung bình'
        WHEN sold_count >= 10 THEN 'Bán ít'
        ELSE 'Bán rất ít'
    END
ORDER BY category, source, 
    CASE 
        WHEN sold_count >= 1000 THEN 1
        WHEN sold_count >= 500 THEN 2
        WHEN sold_count >= 100 THEN 3
        WHEN sold_count >= 10 THEN 4
        ELSE 5
    END;

-- Phân tích correlation giữa các yếu tố và thành công
SELECT 
    category,
    source,
    ROUND(CORR(price, sold_count)::numeric, 3) AS price_sales_corr,
    ROUND(CORR(review_score, sold_count)::numeric, 3) AS review_score_sales_corr,
    ROUND(CORR(review_count, sold_count)::numeric, 3) AS review_count_sales_corr,
    COUNT(*) AS product_count
FROM all_products
WHERE price > 0 AND sold_count >= 0 AND review_count >= 0
GROUP BY category, source
ORDER BY category, source;

-- ============================================
-- 5. CÁC TRUY VẤN INSIGHTS BỔ SUNG
-- ============================================

-- Top 10 sản phẩm bán chạy nhất theo category
SELECT 
    name,
    source,
    category,
    brand,
    price,
    sold_count,
    review_count,
    ROUND(review_score::numeric, 2) AS review_score
FROM all_products
WHERE sold_count > 0
ORDER BY sold_count DESC
LIMIT 10;

-- Top sản phẩm bán chạy theo từng category
WITH ranked_products AS (
    SELECT 
        category,
        source,
        name,
        brand,
        price,
        sold_count,
        review_count,
        ROUND(review_score::numeric, 2) AS review_score,
        ROW_NUMBER() OVER (PARTITION BY category, source ORDER BY sold_count DESC) AS rank_in_category
    FROM all_products
    WHERE sold_count > 0
)
SELECT 
    category,
    source,
    name,
    brand,
    price,
    sold_count,
    review_count,
    review_score,
    rank_in_category
FROM ranked_products
WHERE rank_in_category <= 5
ORDER BY category, source, sold_count DESC;

-- So sánh giá giữa Tiki và Lazada cho cùng một sản phẩm (theo tên tương tự)
-- Sử dụng hàm chuẩn hóa tên để so khớp
WITH normalized_products AS (
    SELECT 
        id,
        name,
        source,
        price,
        sold_count,
        review_count,
        review_score,
        brand,
        category,
        -- Chuẩn hóa tên sản phẩm để so khớp
        regexp_replace(
            regexp_replace(
                lower(coalesce(name,'')),
                '(ch[íi]nh h[aă]ng|chinh hang|h[aă]ng ch[ií]nh h[aă]ng|h[aă]ng qu[aà]c|ch[á]nh h[a]ng|m[á]y|đ[iệ]n tho[ạ]i|smartphone|phone|l[ă]p ?top|notebook|laptop|tai nghe|bluetooth|tai nghe bluetooth|đ[ấ]m b[ả]o|flash sale|kh[u]y[ế]n ma[ĩ])',
                '',
                'gi'
            ),
            '[^a-z0-9]+',
            '',
            'g'
        ) AS canonical_key
    FROM all_products
)
SELECT 
    t.name AS tiki_name,
    l.name AS lazada_name,
    t.price AS tiki_price,
    l.price AS lazada_price,
    (l.price - t.price) AS price_diff,
    t.sold_count AS tiki_sold,
    l.sold_count AS lazada_sold,
    t.review_count AS tiki_reviews,
    l.review_count AS lazada_reviews,
    ROUND(t.review_score::numeric, 2) AS tiki_score,
    ROUND(l.review_score::numeric, 2) AS lazada_score,
    CASE 
        WHEN t.price > 0 THEN ROUND((ABS(l.price - t.price) / t.price * 100)::numeric, 2)
        ELSE NULL
    END AS price_diff_percent
FROM normalized_products t
JOIN normalized_products l
    ON t.canonical_key = l.canonical_key
    AND t.source = 'Tiki'
    AND l.source = 'Lazada'
WHERE ABS(l.price - t.price) > 100000  -- Chênh lệch giá > 100k
ORDER BY ABS(l.price - t.price) DESC
LIMIT 20;

-- Phân tích theo brand
SELECT 
    brand,
    category,
    source,
    COUNT(*) AS product_count,
    ROUND(AVG(price)::numeric, 0) AS avg_price,
    ROUND(AVG(sold_count)::numeric, 2) AS avg_sold_count,
    ROUND(AVG(review_count)::numeric, 2) AS avg_review_count,
    ROUND(AVG(review_score)::numeric, 2) AS avg_review_score,
    SUM(sold_count) AS total_sold,
    ROUND(MAX(sold_count)::numeric, 0) AS max_sold_count
FROM all_products
WHERE brand != 'Unknown' AND brand IS NOT NULL
GROUP BY brand, category, source
ORDER BY total_sold DESC
LIMIT 30;

-- Sản phẩm có giá tốt nhất (giá thấp nhưng đánh giá cao)
SELECT 
    name,
    source,
    category,
    brand,
    price,
    sold_count,
    review_count,
    ROUND(review_score::numeric, 2) AS review_score,
    ROUND((review_score / NULLIF(price, 0) * 1000000)::numeric, 4) AS value_score  -- Điểm giá trị
FROM all_products
WHERE price > 0 AND review_score >= 4.0 AND review_count >= 10
ORDER BY value_score DESC
LIMIT 20;

-- Phân tích sản phẩm mới (có ít đánh giá nhưng bán tốt)
SELECT 
    name,
    source,
    category,
    brand,
    price,
    sold_count,
    review_count,
    ROUND(review_score::numeric, 2) AS review_score,
    CASE 
        WHEN review_count > 0 THEN ROUND((sold_count::FLOAT / review_count)::numeric, 2)
        ELSE NULL
    END AS sales_per_review
FROM all_products
WHERE sold_count > 100 AND review_count < 20 AND review_count > 0
ORDER BY sales_per_review DESC
LIMIT 20;

-- Thống kê tổng quan
SELECT 
    'Tổng số sản phẩm' AS metric,
    COUNT(*)::text AS value
FROM all_products
UNION ALL
SELECT 
    'Tổng số sản phẩm có đánh giá' AS metric,
    COUNT(*)::text AS value
FROM all_products
WHERE review_count > 0
UNION ALL
SELECT 
    'Tổng số lượng bán' AS metric,
    SUM(sold_count)::text AS value
FROM all_products
UNION ALL
SELECT 
    'Trung bình giá' AS metric,
    ROUND(AVG(price)::numeric, 0)::text AS value
FROM all_products
WHERE price > 0
UNION ALL
SELECT 
    'Trung bình số lượng bán' AS metric,
    ROUND(AVG(sold_count)::numeric, 2)::text AS value
FROM all_products
WHERE sold_count > 0
UNION ALL
SELECT 
    'Trung bình điểm đánh giá' AS metric,
    ROUND(AVG(review_score)::numeric, 2)::text AS value
FROM all_products
WHERE review_score > 0;
