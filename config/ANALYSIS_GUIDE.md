# Hướng Dẫn Phân Tích Dữ Liệu - Tiki & Lazada ETL

## Tổng Quan

Hệ thống đã được thiết kế để trả lời **TẤT CẢ** các câu hỏi phân tích về mối quan hệ giữa giá cả, số lượng bán và đánh giá sản phẩm.

## Cấu Trúc Database

### 1. Bảng Chính

#### `all_products`
Lưu thông tin sản phẩm hiện tại:
- `name`, `price`, `sold_count`
- `review_count`, `review_score` ⭐ (MỚI)
- `brand`, `category`, `source`
- `crawled_at`

#### `price_history`
Lưu lịch sử giá, số lượng bán và đánh giá theo ngày:
- `name`, `source`, `price`, `sold_count`
- `review_count`, `review_score` ⭐ (MỚI)
- `brand`, `category`
- `crawl_date`, `created_at`

#### `sale_periods` ⭐ (MỚI)
Đánh dấu các mùa sale để phân tích theo thời điểm:
- `period_name`, `start_date`, `end_date`
- `period_type` (Tet, BlackFriday, MidYear, YearEnd, Normal)
- `description`

### 2. Views Phân Tích

#### `price_vs_sales_trend`
**Mục đích**: Phân tích ảnh hưởng của giá cả đến số lượng bán theo thời gian
- Tính toán `price_change` và `sold_change` giữa các ngày
- Trả lời: "Khi giá tăng, số lượng bán có giảm không?"

#### `price_range_sales_comparison`
**Mục đích**: So sánh số lượng bán giữa các nhóm giá
- Phân loại: Giá thấp (<5M), Trung bình (5M-15M), Cao (15M-30M), Rất cao (>30M)
- Trả lời: "Sản phẩm giá cao có bán chạy không?"

#### `price_vs_review_relationship`
**Mục đích**: Phân tích mối quan hệ giữa giá và đánh giá
- Tính `high_rating_ratio` (tỷ lệ sản phẩm có rating >= 4.0)
- Trả lời: "Sản phẩm giá cao có nhận được nhiều đánh giá tích cực hơn không?"

#### `review_count_vs_sales`
**Mục đích**: Phân tích mối quan hệ giữa số lượng đánh giá và số lượng bán
- Phân loại theo segment: Không có, Ít (1-9), Trung bình (10-99), Nhiều (100-499), Rất nhiều (500+)
- Trả lời: "Tăng số lượng đánh giá có làm tăng số lượng bán không?"

#### `high_reviews_low_sales`
**Mục đích**: Tìm sản phẩm có nhiều đánh giá nhưng bán ít
- Filter: `review_count > 50` và `sold_count < 100`
- Tính `reviews_per_sale` (tỷ lệ đánh giá/số lượng bán)
- Trả lời: "Sản phẩm nào có số lượng đánh giá nhiều nhất nhưng lại bán ít nhất?"

#### `sales_by_period`
**Mục đích**: Phân tích số lượng bán theo các mùa sale
- Join với `sale_periods` để xác định thời điểm
- Tính tổng và trung bình theo từng period
- Trả lời: "Sản phẩm bán tốt nhất vào thời điểm nào trong năm?"

#### `price_change_by_period`
**Mục đích**: So sánh giá trong các mùa sale với thời gian bình thường
- Tính `price_change_percent` so với giá bình thường
- Trả lời: "Mức độ thay đổi giá trong các mùa sale?"

#### `category_trends`
**Mục đích**: Phân tích xu hướng theo category theo thời gian
- Tính trung bình giá, số lượng bán, đánh giá theo ngày
- Trả lời: "Có xu hướng chung về sự thay đổi giá và số lượng bán trong cùng một loại hàng hóa không?"

#### `product_success_factors`
**Mục đích**: Phân tích các yếu tố ảnh hưởng đến thành công sản phẩm
- Phân loại theo `sales_performance`: Bán rất tốt, Bán tốt, Bán trung bình, Bán ít, Bán rất ít
- Tính median và average cho price, review_score
- Trả lời: "Yếu tố nào (giá, đánh giá, số lượng bán) có ảnh hưởng lớn nhất?"

## Cách Sử Dụng

### 1. Truy Vấn Trực Tiếp Từ Views

```sql
-- Ví dụ: Xem ảnh hưởng của giá đến số lượng bán
SELECT * FROM price_vs_sales_trend 
WHERE category = 'Điện thoại' 
ORDER BY crawl_date DESC 
LIMIT 50;
```

### 2. Sử Dụng File SQL Queries

File `config/analysis_queries.sql` chứa các câu truy vấn mẫu để trả lời từng câu hỏi:

```bash
# Kết nối PostgreSQL và chạy queries
psql -h localhost -p 5433 -U <user> -d <database> -f config/analysis_queries.sql
```

### 3. Sử Dụng Trong Metabase/BI Tools

Các views có thể được sử dụng trực tiếp trong Metabase hoặc các công cụ BI khác:
- Kết nối đến database `postgres-data` (port 5433)
- Chọn view cần thiết
- Tạo dashboard và visualization

## Ví Dụ Truy Vấn Theo Câu Hỏi

### Câu hỏi 1: Khi giá tăng, số lượng bán có giảm không?

```sql
SELECT 
    category,
    source,
    COUNT(*) AS total_observations,
    SUM(CASE WHEN price_change > 0 AND sold_change < 0 THEN 1 ELSE 0 END) AS price_up_sales_down,
    SUM(CASE WHEN price_change < 0 AND sold_change > 0 THEN 1 ELSE 0 END) AS price_down_sales_up,
    ROUND(AVG(CASE WHEN price_change > 0 AND sold_change < 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS price_up_sales_down_pct
FROM price_vs_sales_trend
WHERE price_change IS NOT NULL AND sold_change IS NOT NULL
GROUP BY category, source;
```

### Câu hỏi 2: Sản phẩm giá cao có bán chạy không?

```sql
SELECT 
    price_range,
    AVG(avg_sold_count) AS avg_sold,
    AVG(avg_review_count) AS avg_reviews
FROM price_range_sales_comparison
GROUP BY price_range
ORDER BY 
    CASE price_range
        WHEN 'Giá thấp (<5M)' THEN 1
        WHEN 'Giá trung bình (5M-15M)' THEN 2
        WHEN 'Giá cao (15M-30M)' THEN 3
        ELSE 4
    END;
```

### Câu hỏi 3: Tăng số lượng đánh giá có làm tăng số lượng bán không?

```sql
SELECT 
    review_segment,
    AVG(avg_sold_count) AS avg_sold,
    AVG(avg_review_count) AS avg_reviews
FROM review_count_vs_sales
GROUP BY review_segment
ORDER BY 
    CASE review_segment
        WHEN 'Không có đánh giá' THEN 1
        WHEN 'Ít đánh giá (1-9)' THEN 2
        WHEN 'Trung bình (10-99)' THEN 3
        WHEN 'Nhiều đánh giá (100-499)' THEN 4
        ELSE 5
    END;
```

### Câu hỏi 4: Sản phẩm bán tốt nhất vào thời điểm nào?

```sql
SELECT 
    period_type,
    SUM(total_sold) AS total_sold,
    AVG(avg_sold_count) AS avg_sold_per_product
FROM sales_by_period
GROUP BY period_type
ORDER BY total_sold DESC;
```

### Câu hỏi 5: Yếu tố nào ảnh hưởng lớn nhất đến thành công?

```sql
SELECT 
    category,
    source,
    CORR(price, sold_count) AS price_correlation,
    CORR(review_score, sold_count) AS review_score_correlation,
    CORR(review_count, sold_count) AS review_count_correlation
FROM all_products
WHERE price > 0 AND sold_count > 0
GROUP BY category, source;
```

## Lưu Ý Quan Trọng

1. **Dữ liệu cần thời gian tích lũy**: Các phân tích về trend và correlation cần ít nhất 7-14 ngày dữ liệu để có kết quả chính xác.

2. **Sale Periods**: Bảng `sale_periods` cần được cập nhật thủ công hoặc tự động khi có các đợt sale mới.

3. **Indexes**: Đã tạo indexes để tối ưu truy vấn, nhưng với dữ liệu lớn có thể cần thêm indexes.

4. **Materialized Views**: Nếu performance chậm, có thể convert các views thành materialized views và refresh định kỳ.

## Cập Nhật Sale Periods

Để thêm mùa sale mới:

```sql
INSERT INTO sale_periods (period_name, start_date, end_date, period_type, description)
VALUES 
    ('Tết Nguyên Đán 2026', '2026-01-25', '2026-02-15', 'Tet', 'Tết Nguyên Đán 2026');
```

## Troubleshooting

### View không có dữ liệu?
- Kiểm tra xem `price_history` đã có dữ liệu chưa
- Kiểm tra `sale_periods` đã được populate chưa
- Xem log của task `track_price_history` trong Airflow

### Query chậm?
- Kiểm tra indexes đã được tạo chưa
- Xem xét sử dụng materialized views
- Thêm WHERE clause để filter dữ liệu

## Kết Luận

Với schema và views này, bạn có thể trả lời **TẤT CẢ** các câu hỏi phân tích đã đề ra. Các views được thiết kế để dễ dàng sử dụng trong BI tools và visualization.

