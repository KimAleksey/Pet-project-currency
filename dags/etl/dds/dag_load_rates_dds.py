import logging
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# Конфигурация DAG
OWNER = "kim-av"
DAG_ID = "dag_for_loading_dds_rates"
TAGS = ["example", "etl"]

# Длинное описание
LONG_DESCRIPTION = """
Загрузка данных по курсу переводов денежных средств через сервис Золотая корона.
"""

# Короткое описание
SHORT_DESCRIPTION = "Загрузка данных по курсу переводов денежных средств через сервис Золотая корона."

# Аргументы по-умолчанию
default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=3),
}


def load_rates_to_dds(**context):
    pg_hook = PostgresHook("my_dwh")
    insert_sql = """
        -- Закрываем старые версии, где курс изменился
        UPDATE dds.korona_transfer_rates d
        SET ts_to = o.load_ts
        FROM ods.korona_transfer_rates o
        WHERE d.sending_currency_id = o.sending_currency_id
          AND d.receiving_currency_id = o.receiving_currency_id
          AND d.ts_to = TIMESTAMP '9999-12-31';
        
        -- Вставляем новые версии
        INSERT INTO dds.korona_transfer_rates (
            sending_currency_id,
            receiving_currency_id,
            ts_from,
            exchange_rate
        )
        SELECT 
            o.sending_currency_id,
            o.receiving_currency_id,
            o.load_ts  + INTERVAL '1 second',
            o.exchange_rate
        FROM ods.korona_transfer_rates o
        LEFT JOIN dds.korona_transfer_rates d
            ON o.sending_currency_id = d.sending_currency_id
           AND o.receiving_currency_id = d.receiving_currency_id
           AND d.ts_to = TIMESTAMP '9999-12-31'
        WHERE d.rate_sk IS NULL -- либо курс отличается, если нужно
           OR d.exchange_rate IS DISTINCT FROM o.exchange_rate;
    """
    try:
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_sql)
                logging.info("Загрузка курсов из ODS в DDS успешно выполнена.")
    except Exception as e:
        logging.exception("Ошибка при загрузке курсов из ODS в RAW")
        raise RuntimeError(f"Could not insert using query: {e}")


with DAG(
    dag_id=DAG_ID,
    description=SHORT_DESCRIPTION,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2025, 12, 24),
    catchup=True,
    default_args=default_args,
    tags=TAGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    external_task = ExternalTaskSensor(
        task_id="wait_for_ods_rates_loaded",
        external_dag_id="dag_for_loading_ods_rates",
        external_task_id="end",
        mode="reschedule",
        poke_interval=60,
        timeout=3600
    )

    load_rates_dds = PythonOperator(
        task_id="load_rates_to_dds",
        python_callable=load_rates_to_dds,
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id="end",
        dag=dag
    )

    external_task >> load_rates_dds >> end_task