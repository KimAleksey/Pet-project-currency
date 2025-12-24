import pendulum
import logging

from typing import Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from utils.utils_koronapay_api import KoronaPayApi
from utils.utils_transform import transform_nested_fields_for_md_currency
from utils.utils_transform import transform_nested_fields_for_raw_korona_transfer_rates
from utils.utils_load_to_postgres import save_dict_to_postgres

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# Конфигурация DAG
OWNER = "kim-av"
DAG_ID = "dag_for_loading_data_to_md_raw"
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


def extract(**context) -> dict[Any, Any]:
    """
    Функция, которая получает данные из API и пушит в xcom

    :param context: context выполнения таски
    :return: Dict - результат API
    """
    korona_from_rus_to_geo = KoronaPayApi()
    result = korona_from_rus_to_geo.get_data_from_api()
    logging.info(f"Получены данные: {result}")
    return result


def transform(table: str):
    def transform_dict(**context) -> dict[str, Any] | None:
        """
        Функция, которая забирает dict из xcom, сформированный ранее. И формирует новый xcom, который содержит
        dict с нужными полями и значениями для загрузки.

        :param table: Таблица для загрузки
        :param context: context выполнения таски extract
        :return: Dict - строка для записи
        """
        ti = context.get("ti")
        api_data = ti.xcom_pull("extract")
        if table == "currencies":
            data = transform_nested_fields_for_md_currency(api_data)
        else:
            data = transform_nested_fields_for_raw_korona_transfer_rates(api_data)
        return data
    return transform_dict


def load(table: str):
    def load_data(**context) -> None:
        """
        Функция, которая забирает dict из xcom, сформированный ранее. Из этого значения формирует INSERT query
        и выполняет его.

        :param context: Context выполнения таски transform
        :return: None
        """
        ti = context.get("ti")
        if table == "currencies":
            row = ti.xcom_pull("transform_curr")
            save_dict_to_postgres(
                conn_id="my_dwh",
                schema="md",
                table="currencies",
                dict_row=row,
                keys=["id"],
                attributes=["code", "name"],
                on_conflict_option="update"
            )
        else:
            row = ti.xcom_pull("transform_rate")
            save_dict_to_postgres(
                conn_id="my_dwh",
                schema="raw",
                table="korona_transfer_rates",
                dict_row=row,
            )
    return load_data


with (DAG(
    dag_id=DAG_ID,
    description=SHORT_DESCRIPTION,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2025, 12, 1),
    catchup=False,
    default_args=default_args,
    tags=TAGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag):

    start_task = EmptyOperator(
        task_id="start",
        dag=dag
    )

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_curr_task = PythonOperator(
        task_id="transform_curr",
        python_callable=transform("currencies"),
    )

    transform_rate_task = PythonOperator(
        task_id="transform_rate",
        python_callable=transform("rates"),
    )

    load_curr_task = PythonOperator(
        task_id="load_curr",
        python_callable=load("currencies"),
    )

    load_rate_task = PythonOperator(
        task_id="load_rate",
        python_callable=load("rates"),
    )

    end_task = EmptyOperator(
        task_id="end",
        dag=dag
    )

    start_task >> extract_task
    extract_task >> transform_curr_task >> load_curr_task >> end_task
    extract_task >> transform_rate_task >> load_rate_task >> end_task
