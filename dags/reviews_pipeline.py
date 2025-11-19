from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
from typing import List, Dict
import logging

from extract_reviews import extract_tiki_reviews

SLACK_CONN_ID = "slack_webhook_conn"


@dag(
    dag_id="weekly_reviews_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=['reviews', 'tiki', 'etl']
)
def weekly_reviews_pipeline():
    start = EmptyOperator(task_id="start")

    create_table = PostgresOperator(
        task_id="create_reviews_table",
        postgres_conn_id="postgres_data_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS reviews (
            id SERIAL PRIMARY KEY,
            product_name TEXT,
            review_id_source TEXT,
            rating INT,
            review_text TEXT,
            review_date TEXT,
            reviewer_name TEXT,
            source TEXT,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(review_id_source, source)
        );
        """
    )

    @task(task_id="get_recent_products")
    def get_recent_products() -> List[Dict]:
        """
        Lấy danh sách một số sản phẩm Tiki mới crawl gần đây từ all_products
        (ưu tiên có product_url trong extended runs; nếu không có URL, bỏ qua)
        Ở đây demo: chọn theo tên có 'iPhone' để minh hoạ reviews.
        """
        hook = PostgresHook(postgres_conn_id='postgres_data_conn')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT name 
            FROM all_products 
            WHERE source = 'Tiki' 
            ORDER BY crawled_at DESC 
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{'name': r[0]} for r in rows]

    @task(task_id="extract_reviews_for_products")
    def extract_reviews_task(products: List[Dict]) -> List[Dict]:
        """
        Demo: giả sử có thể dựng product_url từ name (không luôn chính xác).
        Thực tế: nên lưu product_url khi ETL extended để dùng trực tiếp.
        Ở đây lấy reviews cho các tên chứa 'iPhone' để minh hoạ (tối đa 50 mỗi sp).
        """
        results: List[Dict] = []
        for p in products or []:
            name = p.get('name') or ''
            if 'iphone' not in name.lower():
                continue
            # Không có URL chính xác -> bỏ qua thực crawl; chỉ minh hoạ pipeline
            # Nếu có URL: reviews = extract_tiki_reviews(product_url, limit=50)
            # Ở đây không thực crawl do thiếu URL; trả empty để pipeline an toàn.
            reviews: List[Dict] = []
            for r in reviews:
                r['product_name'] = name
                results.append(r)
        logging.info(f"Reviews collected: {len(results)}")
        return results

    @task(task_id="load_reviews")
    def load_reviews_task(reviews: List[Dict]) -> int:
        if not reviews:
            return 0
        hook = PostgresHook(postgres_conn_id='postgres_data_conn')
        conn = hook.get_conn()
        cur = conn.cursor()
        sql = """
        INSERT INTO reviews (product_name, review_id_source, rating, review_text, review_date, reviewer_name, source, crawled_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (review_id_source, source) DO NOTHING
        """
        rows = [
            (
                r.get('product_name'),
                r.get('review_id_source'),
                r.get('rating'),
                r.get('review_text'),
                r.get('review_date'),
                r.get('reviewer_name'),
                r.get('source'),
            )
            for r in reviews
        ]
        try:
            cur.executemany(sql, rows)
            conn.commit()
            return cur.rowcount
        except Exception as e:
            conn.rollback()
            logging.error(f"Insert reviews error: {e}")
            return 0
        finally:
            cur.close()
            conn.close()

    @task(task_id="notify_reviews")
    def notify(count: int):
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        message = f":large_green_circle: Reviews DAG OK | inserted: {count} | run: {ctx.get('ds')}"
        SlackWebhookOperator(
            task_id='send_success_alert_reviews',
            slack_webhook_conn_id=SLACK_CONN_ID,
            message=message,
        ).execute(context=ctx)

    end = EmptyOperator(task_id="end")

    start >> create_table
    prods = get_recent_products()
    revs = extract_reviews_task(prods)
    inserted = load_reviews_task(revs)
    notify(inserted) >> end


weekly_reviews_pipeline()


