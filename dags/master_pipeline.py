"""
Master DAG - Tiki & Lazada Product ETL Pipeline

DAG này điều phối việc crawl dữ liệu sản phẩm từ 2 nguồn chính:
    - Tiki (API-based crawling)
    - Lazada (Selenium-based crawling)

Workflow:
    1. Setup: Tạo database schema (all_products, price_history, sale_periods)
    2. Extract & Transform: Chạy song song ETL cho Tiki và Lazada
    3. Load: Merge dữ liệu vào bảng all_products (upsert)
    4. Track: Ghi snapshot vào price_history để theo dõi trend
    5. Notify: Gửi thông báo thành công/thất bại qua Slack

Features:
    - Parallel processing: Tiki và Lazada crawl đồng thời
    - Upsert logic: Update nếu sản phẩm đã tồn tại
    - Price tracking: Lưu lịch sử giá theo ngày
    - Error handling: Slack notification khi có lỗi
    - Idempotent: An toàn khi retry

Author: Data Team
Schedule: Daily (@daily)
Last Updated: 2025-01-20
"""

import logging
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
from typing import List, Dict

# Import các hàm ETL từ package includes
from etl_tiki import run_tiki_etl
from etl_lazada import run_lazada_etl


# ============================================================================
# CONFIGURATION - Cấu hình constants
# ============================================================================

# Connection IDs (được định nghĩa trong Airflow Connections)
POSTGRES_CONN_ID = "postgres_data_conn"
SLACK_CONN_ID = "slack_webhook_conn"

# Default DAG arguments
DEFAULT_ARGS = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ETL Configuration
TIKI_SEARCH_QUERIES = ["dien thoai", "laptop", "may tinh bang"]
TIKI_PAGES_PER_QUERY = 8

LAZADA_SEARCH_QUERIES = ["dien thoai", "laptop", "may tinh bang"]
LAZADA_PAGES_PER_QUERY = 6
LAZADA_MAX_PRODUCTS_PER_QUERY = 180


# ============================================================================
# SQL QUERIES - Database schema và queries
# ============================================================================

# SQL để tạo bảng all_products và price_history với migration logic
CREATE_TABLES_SQL = """
-- =====================================================================
-- 1. Bảng all_products: Lưu thông tin sản phẩm hiện tại (snapshot)
-- =====================================================================
CREATE TABLE IF NOT EXISTS all_products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price INT,
    sold_count INT DEFAULT 0,
    brand TEXT,
    source TEXT,  -- 'Tiki' hoặc 'Lazada'
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, source)  -- Constraint để tránh duplicate
);

-- Migration: Thêm các cột mới nếu bảng đã tồn tại
DO $$ 
BEGIN
    -- Thêm review_count nếu chưa có
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name='all_products' AND column_name='review_count') THEN
        ALTER TABLE all_products ADD COLUMN review_count INT DEFAULT 0;
    END IF;
    
    -- Thêm review_score nếu chưa có
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name='all_products' AND column_name='review_score') THEN
        ALTER TABLE all_products ADD COLUMN review_score DECIMAL(3,2) DEFAULT 0.0;
    END IF;
    
    -- Thêm category nếu chưa có
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name='all_products' AND column_name='category') THEN
        ALTER TABLE all_products ADD COLUMN category TEXT;
    END IF;
END $$;

-- =====================================================================
-- 2. Bảng price_history: Lưu lịch sử giá và số lượng bán theo ngày
-- =====================================================================
CREATE TABLE IF NOT EXISTS price_history (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    price INT,
    sold_count INT DEFAULT 0,
    crawl_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, source, crawl_date)  -- Mỗi sản phẩm chỉ có 1 snapshot/ngày
);

-- Migration: Thêm các cột mới cho price_history
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name='price_history' AND column_name='review_count') THEN
        ALTER TABLE price_history ADD COLUMN review_count INT DEFAULT 0;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name='price_history' AND column_name='review_score') THEN
        ALTER TABLE price_history ADD COLUMN review_score DECIMAL(3,2) DEFAULT 0.0;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name='price_history' AND column_name='brand') THEN
        ALTER TABLE price_history ADD COLUMN brand TEXT;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name='price_history' AND column_name='category') THEN
        ALTER TABLE price_history ADD COLUMN category TEXT;
    END IF;
END $$;

-- =====================================================================
-- 3. Bảng sale_periods: Đánh dấu các mùa sale để phân tích
-- =====================================================================
CREATE TABLE IF NOT EXISTS sale_periods (
    id SERIAL PRIMARY KEY,
    period_name TEXT NOT NULL UNIQUE,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    period_type TEXT NOT NULL,  -- 'Tet', 'BlackFriday', 'MidYear', 'YearEnd', 'Normal'
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================================
-- 4. Indexes để tối ưu truy vấn
-- =====================================================================
DO $$ 
BEGIN
    -- Indexes cho all_products
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name='all_products' AND column_name='category') THEN
        CREATE INDEX IF NOT EXISTS idx_all_products_category 
            ON all_products(category) WHERE category IS NOT NULL;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name='all_products' AND column_name='brand') THEN
        CREATE INDEX IF NOT EXISTS idx_all_products_brand 
            ON all_products(brand) WHERE brand IS NOT NULL;
    END IF;
    
    CREATE INDEX IF NOT EXISTS idx_all_products_source ON all_products(source);
    
    -- Indexes cho price_history
    CREATE INDEX IF NOT EXISTS idx_price_history_crawl_date ON price_history(crawl_date);
    
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name='price_history' AND column_name='category') THEN
        CREATE INDEX IF NOT EXISTS idx_price_history_category 
            ON price_history(category) WHERE category IS NOT NULL;
    END IF;
    
    CREATE INDEX IF NOT EXISTS idx_price_history_source ON price_history(source);
    
    -- Indexes cho sale_periods
    CREATE INDEX IF NOT EXISTS idx_sale_periods_dates ON sale_periods(start_date, end_date);
END $$;
"""

# SQL để populate sale periods
POPULATE_SALE_PERIODS_SQL = """
-- Populate danh sách các mùa sale trong năm
INSERT INTO sale_periods (period_name, start_date, end_date, period_type, description)
VALUES 
    ('Tết Nguyên Đán 2025', '2025-01-20', '2025-02-10', 'Tet', 'Tết Nguyên Đán - mùa sale lớn nhất trong năm'),
    ('Black Friday 2025', '2025-11-24', '2025-11-30', 'BlackFriday', 'Black Friday - siêu sale cuối năm'),
    ('Mid Year Sale 2025', '2025-06-01', '2025-06-30', 'MidYear', 'Sale giữa năm'),
    ('Year End Sale 2025', '2025-12-01', '2025-12-31', 'YearEnd', 'Sale cuối năm'),
    ('Normal Period Q1 2025', '2025-01-01', '2025-01-19', 'Normal', 'Thời gian bình thường'),
    ('Normal Period Q2 2025', '2025-03-01', '2025-05-31', 'Normal', 'Thời gian bình thường'),
    ('Normal Period Q3 2025', '2025-07-01', '2025-10-31', 'Normal', 'Thời gian bình thường')
ON CONFLICT (period_name) DO NOTHING;
"""


# ============================================================================
# CALLBACK FUNCTIONS - Error handling và notifications
# ============================================================================

def on_failure_callback(context):
    """
    Callback được gọi khi task thất bại.
    
    Gửi notification lên Slack với thông tin chi tiết về lỗi:
        - DAG ID và Task ID bị lỗi
        - Link tới log để debug
        - Execution date
    
    Args:
        context (dict): Airflow context chứa thông tin về task execution
    """
    task_instance = context.get('task_instance')
    dag_run = context.get('dag_run')
    task_id = task_instance.task_id
    dag_id = dag_run.dag_id
    log_url = task_instance.log_url

    message = f"""
    :red_circle: LỖI Pipeline: *{dag_id}*
    *Task*: `{task_id}`
    *Log URL*: <{log_url}| Xem Log>
    """
    
    slack_op = SlackWebhookOperator(
        task_id='send_failure_alert_task',
        slack_webhook_conn_id=SLACK_CONN_ID, 
        message=message,
    )
    slack_op.execute(context=context)


# ============================================================================
# DAG DEFINITION
# ============================================================================

@dag(
    dag_id="master_tiki_lazada_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # Chạy hàng ngày lúc 00:00
    catchup=False,  # Không chạy backfill cho các ngày đã qua
    tags=['master', 'tiki', 'lazada', 'etl'],
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': on_failure_callback  # Gọi callback khi lỗi
    }
)
def master_product_pipeline():
    """
    Master DAG điều phối việc crawl Tiki và Lazada.
    
    Flow:
        start 
        → create_master_table 
        → populate_sale_periods 
        → [run_tiki_task, run_lazada_task] (parallel)
        → combine_and_load_data
        → track_price_history
        → send_success_notification
        → end
    
    Returns:
        DAG object
    """
    
    # ========================================================================
    # Task 1: Start marker
    # ========================================================================
    start = EmptyOperator(task_id="start")

    # ========================================================================
    # Task 2: Tạo database schema
    # ========================================================================
    create_master_table = PostgresOperator(
        task_id="create_master_table",
        postgres_conn_id=POSTGRES_CONN_ID, 
        sql=CREATE_TABLES_SQL
    )

    # ========================================================================
    # Task 3: Extract & Transform - Tiki (API)
    # ========================================================================
    @task(task_id="run_tiki_etl")
    def run_tiki_task() -> List[Dict]:
        """
        Chạy ETL pipeline cho Tiki.
        
        Crawl dữ liệu từ Tiki API cho 3 nhóm sản phẩm chính:
            - Điện thoại
            - Laptop
            - Máy tính bảng
        
        Returns:
            List[Dict]: Danh sách sản phẩm đã được cleaned và transformed
        """
        return run_tiki_etl(
            search_queries=TIKI_SEARCH_QUERIES,
            pages=TIKI_PAGES_PER_QUERY
        )

    # ========================================================================
    # Task 4: Extract & Transform - Lazada (Selenium)
    # ========================================================================
    @task(task_id="run_lazada_etl")
    def run_lazada_task() -> List[Dict]:
        """
        Chạy ETL pipeline cho Lazada.
        
        Crawl dữ liệu từ Lazada bằng Selenium cho 3 nhóm sản phẩm:
            - Điện thoại (180 sản phẩm)
            - Laptop (180 sản phẩm)
            - Máy tính bảng (180 sản phẩm)
        
        Sử dụng concurrent requests để lấy reviews nhanh hơn.
        
        Returns:
            List[Dict]: Danh sách sản phẩm đã được cleaned và transformed
        """
        return run_lazada_etl(
            search_queries=LAZADA_SEARCH_QUERIES,
            pages=LAZADA_PAGES_PER_QUERY,
            max_products_per_query=LAZADA_MAX_PRODUCTS_PER_QUERY,
        )

    # ========================================================================
    # Task 5: Load - Merge dữ liệu vào database
    # ========================================================================
    @task(task_id="combine_and_load_data")
    def combine_and_load(tiki_data: List[Dict], lazada_data: List[Dict]) -> Dict:
        """
        Combine dữ liệu từ Tiki và Lazada, sau đó load vào all_products.
        
        Logic:
            1. Merge dữ liệu từ 2 nguồn
            2. Validate dữ liệu (check required fields)
            3. Upsert vào all_products (ON CONFLICT DO UPDATE)
            4. Fallback: Nếu batch insert lỗi, insert từng row một
        
        Args:
            tiki_data (List[Dict]): Dữ liệu từ Tiki
            lazada_data (List[Dict]): Dữ liệu từ Lazada
        
        Returns:
            Dict: Summary thống kê:
                - tiki_count: Số sản phẩm từ Tiki
                - lazada_count: Số sản phẩm từ Lazada
                - processed_total: Số sản phẩm đã insert/update
                - all_products_total_after: Tổng số sản phẩm trong DB
        """
        logging.info(f"Tiki trả về {len(tiki_data)} sản phẩm.")
        logging.info(f"Lazada trả về {len(lazada_data)} sản phẩm.")
        
        # Merge dữ liệu từ 2 nguồn
        all_data = (tiki_data or []) + (lazada_data or [])
        
        if not all_data:
            logging.warning("Không có dữ liệu nào để load. Dừng.")
            return {
                "tiki_count": len(tiki_data),
                "lazada_count": len(lazada_data),
                "processed_total": 0,
                "all_products_total_after": 0
            }

        logging.info(f"Tổng cộng {len(all_data)} sản phẩm sẽ được load.")
        
        # Kết nối database
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Chuẩn bị dữ liệu để insert (validate và extract fields)
        rows_to_insert = []
        for product in all_data:
            if product is None:
                continue
            
            try:
                rows_to_insert.append((
                    product['name'], 
                    product['price'], 
                    product['sold_count'],
                    product.get('review_count', 0),
                    product.get('review_score', 0.0),
                    product['brand'], 
                    product['category'], 
                    product['source']
                ))
            except KeyError as e:
                logging.warning(f"Lỗi: Sản phẩm thiếu field {e}: {product}")
                continue
        
        if not rows_to_insert:
            logging.warning("Không có dữ liệu hợp lệ để insert.")
            cursor.close()
            conn.close()
            return {
                "tiki_count": len(tiki_data),
                "lazada_count": len(lazada_data),
                "processed_total": 0,
                "all_products_total_after": 0
            }
        
        # SQL upsert: Insert hoặc update nếu đã tồn tại
        insert_sql = """
        INSERT INTO all_products (name, price, sold_count, review_count, review_score, brand, category, source, crawled_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (name, source) 
        DO UPDATE SET 
            price = EXCLUDED.price,
            sold_count = EXCLUDED.sold_count,
            review_count = EXCLUDED.review_count,
            review_score = EXCLUDED.review_score,
            brand = EXCLUDED.brand,
            category = EXCLUDED.category,
            crawled_at = CURRENT_TIMESTAMP
        """
        
        processed = 0
        total_after = 0
        
        try:
            # Batch insert (nhanh hơn nhiều so với từng row)
            cursor.executemany(insert_sql, rows_to_insert)
            conn.commit()
            processed = len(rows_to_insert)
            logging.info(f"Load thành công {processed} bản ghi (insert/update) vào 'all_products'.")
        except Exception as e:
            # Fallback: Insert từng row nếu batch failed
            conn.rollback()
            logging.error(f"Lỗi khi batch insert: {e}. Thử insert từng row...")
            processed = 0
            
            for row in rows_to_insert:
                try:
                    cursor.execute(insert_sql, row)
                    processed += 1
                except Exception as row_error:
                    logging.warning(f"Lỗi khi insert row {row[0]}: {row_error}")
            
            conn.commit()
            logging.info(f"Load thành công {processed}/{len(rows_to_insert)} bản ghi vào 'all_products'.")
        finally:
            # Lấy tổng số sản phẩm sau khi load
            try:
                cursor.execute("SELECT COUNT(*) FROM all_products;")
                total_after = cursor.fetchone()[0]
            except Exception:
                total_after = 0
            
            cursor.close()
            conn.close()
        
        return {
            "tiki_count": len(tiki_data),
            "lazada_count": len(lazada_data),
            "processed_total": processed,
            "all_products_total_after": total_after
        }

    # ========================================================================
    # Task 6: Track price history - Snapshot hàng ngày
    # ========================================================================
    @task(task_id="track_price_history")
    def track_price_history(load_summary: Dict) -> int:
        """
        Ghi snapshot giá và số lượng bán vào price_history.
        
        Mỗi ngày sẽ lưu 1 snapshot của tất cả sản phẩm trong all_products.
        Dùng để phân tích trend giá và số lượng bán theo thời gian.
        
        Args:
            load_summary (Dict): Summary từ task combine_and_load
        
        Returns:
            int: Số bản ghi đã insert vào price_history
        """
        processed_total = int(load_summary.get("processed_total", 0) or 0)
        
        if processed_total <= 0:
            logging.info("Không có sản phẩm mới để ghi lịch sử giá hôm nay.")
            return 0
        
        # Lấy execution date từ context
        from airflow.operators.python import get_current_context
        context = get_current_context()
        ds = context.get('ds')  # Format: 'YYYY-MM-DD'

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Insert snapshot từ all_products vào price_history
            # ON CONFLICT DO NOTHING: Tránh duplicate nếu chạy lại cùng ngày
            insert_history_sql = """
            INSERT INTO price_history 
                (name, source, price, sold_count, review_count, review_score, brand, category, crawl_date, created_at)
            SELECT 
                ap.name, ap.source, ap.price, ap.sold_count, 
                ap.review_count, ap.review_score, ap.brand, ap.category, 
                %s::date AS crawl_date, CURRENT_TIMESTAMP
            FROM all_products ap
            ON CONFLICT (name, source, crawl_date) DO NOTHING
            """
            
            cursor.execute(insert_history_sql, (ds,))
            inserted = cursor.rowcount
            conn.commit()
            
            logging.info(f"Đã ghi lịch sử giá và đánh giá cho {inserted} sản phẩm (nếu chưa tồn tại).")
            return inserted
        except Exception as e:
            conn.rollback()
            logging.error(f"Lỗi khi ghi price_history: {e}")
            return 0
        finally:
            cursor.close()
            conn.close()

    # ========================================================================
    # Task 7: Gửi thông báo thành công qua Slack
    # ========================================================================
    @task(task_id="send_success_notification")
    def send_success_alert(load_summary: Dict, history_added: int):
        """
        Gửi notification thành công lên Slack.
        
        Thông tin gửi đi:
            - Execution date
            - Số lượng sản phẩm từ Tiki và Lazada
            - Tổng số sản phẩm đã processed
            - Tổng số sản phẩm trong database
            - Số snapshot thêm vào price_history
        
        Args:
            load_summary (Dict): Summary từ task combine_and_load
            history_added (int): Số bản ghi thêm vào price_history
        """
        from airflow.operators.python import get_current_context
        context = get_current_context()
        execution_date = context.get('ds', 'N/A')
        
        message = f"""
        :large_green_circle: Pipeline Tiki/Lazada THÀNH CÔNG
        *DAG*: `master_tiki_lazada_pipeline`
        *Ngày thực thi*: `{execution_date}`
        *Tiki (processed)*: `{load_summary.get('tiki_count', 0)}`
        *Lazada (total)*: `{load_summary.get('lazada_count', 0)}`
        *Tổng processed (insert/update)*: `{load_summary.get('processed_total', 0)}`
        *Tổng trong all_products (sau load)*: `{load_summary.get('all_products_total_after', 0)}`
        *Snapshot thêm vào price_history hôm nay*: `{history_added}`
        """
        
        slack_op = SlackWebhookOperator(
            task_id='send_success_alert_task',
            slack_webhook_conn_id=SLACK_CONN_ID,
            message=message,
        )
        slack_op.execute(context=context)

    # ========================================================================
    # Task 8: Populate sale periods (one-time setup)
    # ========================================================================
    populate_sale_periods = PostgresOperator(
        task_id="populate_sale_periods",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=POPULATE_SALE_PERIODS_SQL
    )

    # ========================================================================
    # Task 9: End marker
    # ========================================================================
    end = EmptyOperator(task_id="end")

    # ========================================================================
    # DEFINE TASK DEPENDENCIES
    # ========================================================================
    
    # Step 1: Setup database
    start >> create_master_table >> populate_sale_periods
    
    # Step 2: Parallel ETL cho Tiki và Lazada
    tiki_data = run_tiki_task()
    lazada_data = run_lazada_task()
    parallel_tasks = [tiki_data, lazada_data]
    populate_sale_periods >> parallel_tasks
    
    # Step 3: Load và track history
    load_summary = combine_and_load(tiki_data=tiki_data, lazada_data=lazada_data)
    history_count = track_price_history(load_summary=load_summary)
    
    # Step 4: Notification và kết thúc
    history_count >> send_success_alert(load_summary=load_summary, history_added=history_count) >> end


# ============================================================================
# DAG INSTANTIATION
# ============================================================================

# Khởi tạo DAG instance để Airflow có thể discover
master_product_pipeline()
