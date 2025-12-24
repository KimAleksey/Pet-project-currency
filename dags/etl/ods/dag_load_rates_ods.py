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
DAG_ID = "dag_for_loading_ods_rates"
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


def load_rates_to_ods(**context):
    pg_hook = PostgresHook("my_dwh")
    insert_sql = """
        INSERT INTO ods.korona_transfer_rates AS ods
        SELECT DISTINCT ON (sending_currency_id, receiving_currency_id)
            sending_currency_id,
            receiving_currency_id,
            NOW() AS load_ts,
            exchange_rate
        FROM raw.korona_transfer_rates
        ORDER BY sending_currency_id, receiving_currency_id, load_ts DESC
        ON CONFLICT (sending_currency_id, receiving_currency_id)
        DO UPDATE
        SET
            load_ts = NOW(),
            exchange_rate = EXCLUDED.exchange_rate;
    """
    try:
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_sql)
                logging.info("Загрузка курсов из RAW в ODS успешно выполнена.")
    except Exception as e:
        logging.exception("Ошибка при загрузке курсов из RAW в ODS")
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
        task_id="wait_for_raw_rates_loaded",
        external_dag_id="dag_for_loading_data_to_md_raw",
        external_task_id="end",
        mode="reschedule",
        poke_interval=60,
        timeout=3600
    )

    load_rates_ods = PythonOperator(
        task_id="load_rates_to_ods",
        python_callable=load_rates_to_ods,
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id="end",
        dag=dag
    )

    external_task >> load_rates_ods >> end_task