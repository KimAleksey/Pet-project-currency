import airflow.models.connection

import logging

from typing import Any

from utils.utils_dict import generate_fields_from_dict
from utils.utils_dict import generate_fields_from_dict_in_placeholder
from utils.utils_str import generate_insert_into_for_row
from utils.utils_str import generate_on_conflict

from airflow.providers.postgres.hooks.postgres import PostgresHook


def generate_insert_sql(
        schema: str | None = "public",
        table: str | None = None,
        columns: str | None = None,
        placeholders: str | None = None,
        keys: list[str] | None = None,
        attributes: list[str] | None = None,
        on_conflict_option: str | None = "nothing"
):
    """

    :param placeholders: Поля для вставки с плейсхолдерами.
    :param columns: Поля для вставки.
    :param schema: Схема, в которую загружаем данные.
    :param table: Имя таблицы, в которую загружаем данные.
    :param keys: Список ключей таблицы.
    :param attributes: Список значений, которые нужно переопределить.
    :param on_conflict_option: Что делать, по-умолчанию 'nothing'. Есть опция update, nothing.
    :return: Строка для вставки.
    """
    insert_sql = generate_insert_into_for_row(
        schema=schema,
        table=table,
        columns=columns,
        placeholders=placeholders
    )

    md_on_conflict = generate_on_conflict(
        keys=keys,
        attributes=attributes,
        on_conflict_option=on_conflict_option
    )

    result = insert_sql + " " + md_on_conflict
    return result


def save_dict_to_postgres(
        conn_id: str | airflow.models.connection.Connection | None = "my_db",
        schema: str | None = "public",
        table: str | None = None,
        dict_row: dict[str, Any] | None = None,
        keys: list[str] | None = None,
        attributes: list[str] | None = None,
        on_conflict_option: str | None = "nothing"
) -> None:
    """
    Функция для вставки одной строки в любую таблицу Postgres.

    :param conn_id: Название connection Airflow
    :param schema: Схема, в которую загружаем данные.
    :param table: Имя таблицы, в которую загружаем данные.
    :param dict_row: Произвольный словарь, который представляет собой строку для записи.
    :param keys: Список ключей таблицы.
    :param attributes: Список значений, которые нужно переопределить.
    :param on_conflict_option: Что делать, по-умолчанию 'nothing'. Есть опция update, nothing.
    """
    # Подключение к БД. Если передается airflow.connection, то значит запускается интеграционное тестирование
    if isinstance(conn_id, airflow.models.connection.Connection):
        pg_hook = PostgresHook(postgres_conn_id=None, connection=conn_id)
    else:
        try:
            pg_hook = PostgresHook(conn_id)
        except Exception as e:
            raise RuntimeError(f"Could not connect to Postgres.") from e

    if not dict_row:
        raise ValueError("dict_row не может быть пустым")

    columns = generate_fields_from_dict(dict_row)
    placeholders = generate_fields_from_dict_in_placeholder(dict_row)
    insert_sql = generate_insert_sql(
        schema=schema,
        table=table,
        columns=columns,
        placeholders=placeholders,
        keys=keys,
        attributes=attributes,
        on_conflict_option=on_conflict_option
    )

    try:
        pg_hook.run(sql=insert_sql, parameters=dict_row, autocommit=True)
        logging.info(f"Запрос был успешно выполнен. Выполняемый запрос: {insert_sql}")
    except Exception as e:
        raise RuntimeError(f"Could not insert into {schema}.{table} using query {insert_sql}: {e}")