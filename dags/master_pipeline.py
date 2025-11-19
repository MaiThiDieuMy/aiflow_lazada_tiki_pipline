# dags/master_pipeline.py
import logging # Đã thêm
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
from typing import List, Dict

# Import thẳng 2 hàm ETL chính từ 2 file 
from etl_tiki import run_tiki_etl
from etl_lazada import run_lazada_etl

# --- Cấu hình Slack ---
SLACK_CONN_ID = "slack_webhook_conn"

def on_failure_callback(context):
    """Gửi thông báo LỖI lên Slack"""
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


# --- Định nghĩa DAG ---
@dag(
    dag_id="master_tiki_lazada_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily", 
    catchup=False,
    tags=['master', 'tiki', 'lazada', 'etl'],
    default_args={
        'owner': 'data_team',
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'on_failure_callback': on_failure_callback 
    }
)
def master_product_pipeline():
    """
    DAG tổng điều phối việc crawl Tiki (API) và Lazada (Selenium),
    sau đó gộp chung dữ liệu vào một bảng 'all_products'.
    """
    
    start = EmptyOperator(task_id="start")

    create_master_table = PostgresOperator(
        task_id="create_master_table",
        postgres_conn_id="postgres_data_conn", 
        sql="""
        CREATE TABLE IF NOT EXISTS all_products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            price INT,
            sold_count INT DEFAULT 0,
            brand TEXT,
            category TEXT,
            source TEXT, 
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, source) 
        );
        
        -- Lưu ảnh chụp giá theo ngày để so sánh trend
        CREATE TABLE IF NOT EXISTS price_history (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            source TEXT NOT NULL,
            price INT,
            sold_count INT DEFAULT 0,
            crawl_date DATE DEFAULT CURRENT_DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, source, crawl_date)
        );
        """
    )

    create_canonical_views = PostgresOperator(
        task_id="create_canonical_views",
        postgres_conn_id="postgres_data_conn",
        sql="""
        -- Hàm chuẩn hóa tên sản phẩm để so khớp cross-source
        CREATE OR REPLACE FUNCTION normalize_product_name(input TEXT)
        RETURNS TEXT
        LANGUAGE sql
        IMMUTABLE
        AS $$
        SELECT regexp_replace(
                 regexp_replace(
                   lower(coalesce(input,'')),
                   '(ch[íi]nh h[aă]ng|chinh hang|h[aă]ng ch[ií]nh h[aă]ng|h[aă]ng qu[aà]c|ch[á]nh h[a]ng|m[á]y|đ[iệ]n tho[ạ]i|smartphone|phone|l[ă]p ?top|notebook|laptop|tai nghe|bluetooth|tai nghe bluetooth|đ[ấ]m b[ả]o|flash sale|kh[u]y[ế]n ma[ĩ])',
                   '',
                   'gi'
                 ),
                 '[^a-z0-9]+',
                 '',
                 'g'
               );
        $$;

        -- View chuẩn hóa để join theo canonical_key
        CREATE OR REPLACE VIEW product_canonical AS
        SELECT 
          id,
          name,
          source,
          price,
          sold_count,
          brand,
          category,
          crawled_at,
          normalize_product_name(name) AS canonical_key
        FROM all_products;

        -- View so sánh giá giữa Tiki và Lazada theo canonical_key
        CREATE OR REPLACE VIEW cross_source_price_compare AS
        SELECT 
          t.canonical_key,
          t.name  AS tiki_name,
          l.name  AS lazada_name,
          t.price AS tiki_price,
          l.price AS lazada_price,
          (l.price - t.price) AS price_diff,
          t.sold_count AS tiki_sold,
          l.sold_count AS lazada_sold,
          greatest(t.crawled_at, l.crawled_at) AS last_seen
        FROM product_canonical t
        JOIN product_canonical l
          ON t.canonical_key = l.canonical_key
         AND t.source = 'Tiki'
         AND l.source = 'Lazada';
        """
    )

    @task(task_id="run_tiki_etl")
    def run_tiki_task() -> List[Dict]:
        return run_tiki_etl()

    @task(task_id="run_lazada_phones_etl")
    def run_lazada_phones_task() -> List[Dict]:
        # Crawl danh mục điện thoại nói chung
        return run_lazada_etl(search_query="dien thoai", pages=8)

    @task(task_id="run_lazada_laptops_etl")
    def run_lazada_laptops_task() -> List[Dict]:
        # Crawl danh mục laptop
        return run_lazada_etl(search_query="laptop", pages=8)

    @task(task_id="combine_and_load_data")
    def combine_and_load(tiki_data: List[Dict], lazada_phones: List[Dict], lazada_laptops: List[Dict]):
        logging.info(f"Tiki trả về {len(tiki_data)} sản phẩm.")
        logging.info(f"Lazada (điện thoại) trả về {len(lazada_phones)} sản phẩm.")
        logging.info(f"Lazada (laptop) trả về {len(lazada_laptops)} sản phẩm.")
        
        lazada_data_all = (lazada_phones or []) + (lazada_laptops or [])
        all_data = (tiki_data or []) + lazada_data_all
        
        if not all_data:
            logging.warning("Không có dữ liệu nào để load. Dừng.")
            return {
                "tiki_count": len(tiki_data),
                "lazada_count": len(lazada_data_all),
                "lazada_phones": len(lazada_phones),
                "lazada_laptops": len(lazada_laptops),
                "processed_total": 0,
                "all_products_total_after": 0
            }

        logging.info(f"Tổng cộng {len(all_data)} sản phẩm sẽ được load.")
        
        hook = PostgresHook(postgres_conn_id='postgres_data_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Chuẩn bị dữ liệu để insert
        rows_to_insert = []
        for product in all_data:
            if product is None:
                continue
            try:
                rows_to_insert.append((
                    product['name'],
                    product['price'],
                    product['sold_count'],
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
            return 0
        
        # Sử dụng ON CONFLICT để update nếu đã tồn tại (dựa trên name, source)
        insert_sql = """
        INSERT INTO all_products (name, price, sold_count, brand, category, source, crawled_at)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (name, source) 
        DO UPDATE SET 
            price = EXCLUDED.price,
            sold_count = EXCLUDED.sold_count,
            brand = EXCLUDED.brand,
            category = EXCLUDED.category,
            crawled_at = CURRENT_TIMESTAMP
        """
        processed = 0
        total_after = 0
        try:
            # Sử dụng executemany để insert nhiều rows cùng lúc (nhanh hơn)
            cursor.executemany(insert_sql, rows_to_insert)
            conn.commit()
            processed = len(rows_to_insert)
            logging.info(f"Load thành công {processed} bản ghi (insert/update) vào 'all_products'.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Lỗi khi insert vào database: {e}")
            # Fallback: thử insert từng dòng để xem dòng nào bị lỗi
            logging.info("Thử insert từng dòng để tìm lỗi...")
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
            # Lấy tổng số sản phẩm hiện có sau khi load
            try:
                cursor.execute("SELECT COUNT(*) FROM all_products;")
                total_after = cursor.fetchone()[0]
            except Exception:
                total_after = 0
            cursor.close()
            conn.close()
        
        return {
            "tiki_count": len(tiki_data),
            "lazada_count": len(lazada_data_all),
            "lazada_phones": len(lazada_phones),
            "lazada_laptops": len(lazada_laptops),
            "processed_total": processed,
            "all_products_total_after": total_after
        }

    @task(task_id="track_price_history")
    def track_price_history(load_summary: Dict):
        """
        Ghi nhận snapshot giá mỗi ngày vào bảng price_history (append-only).
        Dùng logical execution date (ds) của Airflow để hỗ trợ backfill.
        """
        processed_total = int(load_summary.get("processed_total", 0) or 0)
        if processed_total <= 0:
            logging.info("Không có sản phẩm mới để ghi lịch sử giá hôm nay.")
            return 0
        from airflow.operators.python import get_current_context
        context = get_current_context()
        ds = context.get('ds')  # 'YYYY-MM-DD'

        hook = PostgresHook(postgres_conn_id='postgres_data_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            insert_history_sql = """
            INSERT INTO price_history (name, source, price, sold_count, crawl_date, created_at)
            SELECT 
                ap.name,
                ap.source,
                ap.price,
                ap.sold_count,
                %s::date AS crawl_date,
                CURRENT_TIMESTAMP
            FROM all_products ap
            ON CONFLICT (name, source, crawl_date) DO NOTHING
            """
            cursor.execute(insert_history_sql, (ds,))
            inserted = cursor.rowcount
            conn.commit()
            logging.info(f"Đã ghi lịch sử giá cho {inserted} sản phẩm (nếu chưa tồn tại).")
            return inserted
        except Exception as e:
            conn.rollback()
            logging.error(f"Lỗi khi ghi price_history: {e}")
            return 0
        finally:
            cursor.close()
            conn.close()

    @task(task_id="send_success_notification")
    def send_success_alert(load_summary: Dict, history_added: int):
        from airflow.operators.python import get_current_context
        context = get_current_context()
        execution_date = context.get('ds', 'N/A')
        
        message = f"""
        :large_green_circle: Pipeline Tiki/Lazada THÀNH CÔNG
        *DAG*: `master_tiki_lazada_pipeline`
        *Ngày thực thi*: `{execution_date}`
        *Tiki (processed)*: `{load_summary.get('tiki_count', 0)}`
        *Lazada (phones)*: `{load_summary.get('lazada_phones', 0)}`
        *Lazada (laptops)*: `{load_summary.get('lazada_laptops', 0)}`
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

    end = EmptyOperator(task_id="end")

    # --- Định nghĩa luồng chạy (Dependencies) ---
    tiki_data = run_tiki_task()
    lazada_phones = run_lazada_phones_task()
    lazada_laptops = run_lazada_laptops_task()
    
    parallel_tasks = [tiki_data, lazada_phones, lazada_laptops]
    start >> create_master_table >> create_canonical_views >> parallel_tasks

    # ĐÃ SỬA LỖI: 'tili_data' -> 'tiki_data'
    load_summary = combine_and_load(tiki_data=tiki_data, lazada_phones=lazada_phones, lazada_laptops=lazada_laptops)
    
    # Ghi lịch sử giá theo ngày trước khi gửi thông báo
    history_count = track_price_history(load_summary=load_summary)
    history_count >> send_success_alert(load_summary=load_summary, history_added=history_count) >> end

# Khởi tạo DAG
master_product_pipeline()