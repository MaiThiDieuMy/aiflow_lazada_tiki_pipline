from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
from typing import List, Dict
import logging

# === SỬA IMPORT: Không còn 'config.' hay 'scripts.' ===
from crawl_config import get_crawl_config_for_today
from etl_tiki_extended import run_tiki_etl_extended
from etl_lazada_extended import run_lazada_etl_extended

SLACK_CONN_ID = "slack_webhook_conn"


def on_failure_callback(context):
    task_instance = context.get('task_instance')
    dag_run = context.get('dag_run')
    message = f"""
    :red_circle: LỖI Pipeline (rotation): *{dag_run.dag_id}*
    *Task*: `{task_instance.task_id}`
    *Log URL*: <{task_instance.log_url}| Xem Log>
    """
    SlackWebhookOperator(
        task_id='send_failure_alert_rotation',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
    ).execute(context=context)


@dag(
    dag_id="master_tiki_lazada_pipeline_rotation",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=['master', 'rotation', 'tiki', 'lazada', 'etl'],
    default_args={
        'owner': 'data_team',
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'on_failure_callback': on_failure_callback
    }
)
def master_pipeline_rotation():
    start = EmptyOperator(task_id="start")

    bootstrap = PostgresOperator(
        task_id="bootstrap_tables",
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
        task_id="create_canonical_views_rotation",
        postgres_conn_id="postgres_data_conn",
        sql="""
        CREATE OR REPLACE FUNCTION normalize_product_name(input TEXT)
        RETURNS TEXT
        LANGUAGE sql
        IMMUTABLE
        AS $$
        SELECT regexp_replace(
                 regexp_replace(
                   lower(coalesce(input,'')),
                   '(ch[íi]nh h[aă]ng|chinh hang|h[aă]ng ch[ií]nh h[aă]ng|h[ă]ng qu[à]c|m[á]y|đ[iệ]n tho[ạ]i|smartphone|phone|l[ă]p ?top|notebook|laptop|tai nghe|bluetooth|tai nghe bluetooth|flash sale|kh[u]y[ế]n ma[ĩ])',
                   '',
                   'gi'
                 ),
                 '[^a-z0-9]+',
                 '',
                 'g'
               );
        $$;

        CREATE OR REPLACE VIEW product_canonical AS
        SELECT 
          id, name, source, price, sold_count, brand, category, crawled_at,
          normalize_product_name(name) AS canonical_key
        FROM all_products;

        CREATE OR REPLACE VIEW cross_source_price_compare AS
        SELECT 
          t.canonical_key,
          t.name  AS tiki_name,
          l.name  AS lazada_name,
          t.price AS tiki_price,
          l.price AS lazada_price,
          (l.price - t.price) AS price_diff,
          greatest(t.crawled_at, l.crawled_at) AS last_seen
        FROM product_canonical t
        JOIN product_canonical l
          ON t.canonical_key = l.canonical_key
         AND t.source = 'Tiki'
         AND l.source = 'Lazada';
        """
    )

    @task(task_id="get_rotation_config")
    def get_rotation_config() -> Dict:
        cfg = get_crawl_config_for_today()
        logging.info(f"Rotation config: {cfg}")
        return cfg

    @task(task_id="run_tiki_extended")
    def tiki_task(cfg: Dict) -> List[Dict]:
        tconf = cfg['tiki']
        return run_tiki_etl_extended(
            category=tconf['category'],
            url_key=tconf['url_key'],
            pages=tconf['pages']
        )

    @task(task_id="run_lazada_extended")
    def lazada_task(cfg: Dict) -> List[Dict]:
        lconf = cfg['lazada']
        return run_lazada_etl_extended(
            search_query=lconf['query'],
            pages=lconf['pages']
        )

    @task(task_id="load_minimal_columns")
    def load_minimal(tiki_data: List[Dict], lazada_data: List[Dict]) -> int:
        all_data = []
        for p in (tiki_data or []):
            all_data.append({
                'name': p.get('name'),
                'price': p.get('price'),
                'sold_count': p.get('sold_count'),
                'brand': None,
                'category': None,
                'source': 'Tiki'
            })
        for p in (lazada_data or []):
            all_data.append({
                'name': p.get('name'),
                'price': p.get('price'),
                'sold_count': p.get('sold_count'),
                'brand': None,
                'category': None,
                'source': 'Lazada'
            })
        if not all_data:
            return 0
        hook = PostgresHook(postgres_conn_id='postgres_data_conn')
        conn = hook.get_conn()
        cur = conn.cursor()
        insert_sql = """
        INSERT INTO all_products (name, price, sold_count, brand, category, source, crawled_at)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (name, source) DO UPDATE SET
            price = EXCLUDED.price,
            sold_count = EXCLUDED.sold_count,
            brand = EXCLUDED.brand,
            category = EXCLUDED.category,
            crawled_at = CURRENT_TIMESTAMP
        """
        rows = [(i['name'], i['price'], i['sold_count'], i['brand'], i['category'], i['source']) for i in all_data if i.get('name') is not None]
        try:
            cur.executemany(insert_sql, rows)
            conn.commit()
            return len(rows)
        except Exception as e:
            conn.rollback()
            logging.error(f"Insert all_products error: {e}")
            return 0
        finally:
            cur.close()
            conn.close()

    @task(task_id="track_price_history_rotation")
    def track_history(loaded: int) -> int:
        if not loaded or loaded <= 0:
            return 0
        hook = PostgresHook(postgres_conn_id='postgres_data_conn')
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
            INSERT INTO price_history (name, source, price, sold_count, crawl_date, created_at)
            SELECT name, source, price, sold_count, CURRENT_DATE, CURRENT_TIMESTAMP
            FROM all_products
            WHERE DATE(crawled_at) = CURRENT_DATE
            ON CONFLICT (name, source, crawl_date) DO NOTHING
            """)
            cnt = cur.rowcount
            conn.commit()
            return cnt
        except Exception as e:
            conn.rollback()
            logging.error(f"Insert price_history error: {e}")
            return 0
        finally:
            cur.close()
            conn.close()

    @task(task_id="notify_rotation")
    def notify(total_loaded: int, history_added: int):
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        message = f"""
        :large_green_circle: Rotation DAG OK
        Loaded: {total_loaded} | History added: {history_added}
        Run: {ctx.get('ds')}
        """
        SlackWebhookOperator(
            task_id='send_success_alert_rotation',
            slack_webhook_conn_id=SLACK_CONN_ID,
            message=message,
        ).execute(context=ctx)

    end = EmptyOperator(task_id="end")

    cfg = get_rotation_config()
    start >> bootstrap >> create_canonical_views >> cfg
    tiki_out = tiki_task(cfg)
    lazada_out = lazada_task(cfg)
    loaded = load_minimal(tiki_out, lazada_out)
    hist = track_history(loaded)
    notify(loaded, hist) >> end


master_pipeline_rotation()


