from handles.oltp.execute_custom_query import execute_custom_query
from handles.oltp.get_postgres_credentials import get_postgres_rec

if __name__ == "__main__":
    # Получаем путь до файла с реквизитами подключения к PG
    pg_credentials = get_postgres_rec()

    # Запрос на создание таблицы с валютами
    query_for_currency_table = """
        CREATE TABLE IF NOT EXISTS md.currencies (
            id INT PRIMARY KEY,
            code TEXT NOT NULL,
            name TEXT NOT NULL
        );
    """

    # Запрос на создание таблицы с курсами в raw
    query_for_raw_rates_table = """
        CREATE TABLE IF NOT EXISTS raw.korona_transfer_rates (
            raw_id BIGSERIAL PRIMARY KEY,
            sending_currency_id INT NOT NULL REFERENCES md.currencies(id),
            receiving_currency_id INT NOT NULL REFERENCES md.currencies(id),
            load_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            exchange_rate NUMERIC(18, 6) NOT NULL
        );
    """

    # Запрос на создание таблицы с курсами в ods
    query_for_ods_rates_table = """
        CREATE TABLE IF NOT EXISTS ods.korona_transfer_rates (
            sending_currency_id INT NOT NULL REFERENCES md.currencies(id),
            receiving_currency_id INT NOT NULL REFERENCES md.currencies(id),
            load_ts timestamp NOT NULL,
            exchange_rate NUMERIC(18, 6) NOT NULL,
            PRIMARY KEY (sending_currency_id, receiving_currency_id)
        );
    """

    # Запрос на создание таблицы с курсами в dds
    query_for_dds_rates_table = """
        CREATE TABLE IF NOT EXISTS dds.korona_transfer_rates (
            rate_sk BIGSERIAL PRIMARY KEY,
            sending_currency_id INT NOT NULL REFERENCES md.currencies(id),
            receiving_currency_id INT NOT NULL REFERENCES md.currencies(id),
            ts_from timestamp NOT NULL,
            ts_to timestamp NOT NULL DEFAULT TIMESTAMP '9999-12-31',
            exchange_rate NUMERIC(18, 6) NOT NULL,
            UNIQUE (sending_currency_id, receiving_currency_id, ts_from)
        );
    """

    # Создание таблицы с валютами
    execute_custom_query(
        db_name=pg_credentials["db_name"],
        port=pg_credentials["port"],
        host=pg_credentials["host"],
        user=pg_credentials["user"],
        password=pg_credentials["password"],
        query = query_for_currency_table
    )

    # Создание таблицы с курсами в raw
    execute_custom_query(
        db_name=pg_credentials["db_name"],
        port=pg_credentials["port"],
        host=pg_credentials["host"],
        user=pg_credentials["user"],
        password=pg_credentials["password"],
        query = query_for_raw_rates_table
    )

    # Создание таблицы с курсами в raw
    execute_custom_query(
        db_name=pg_credentials["db_name"],
        port=pg_credentials["port"],
        host=pg_credentials["host"],
        user=pg_credentials["user"],
        password=pg_credentials["password"],
        query = query_for_ods_rates_table
    )

    # Создание таблицы с курсами в raw
    execute_custom_query(
        db_name=pg_credentials["db_name"],
        port=pg_credentials["port"],
        host=pg_credentials["host"],
        user=pg_credentials["user"],
        password=pg_credentials["password"],
        query = query_for_dds_rates_table
    )