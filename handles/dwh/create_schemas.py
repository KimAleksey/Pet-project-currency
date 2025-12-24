from handles.oltp.execute_custom_query import execute_custom_query
from handles.oltp.get_postgres_credentials import get_postgres_rec

if __name__ == "__main__":

    # Получаем путь до файла с реквизитами подключения к PG
    pg_credentials = get_postgres_rec()

    # Создаем схему
    execute_custom_query(
        db_name=pg_credentials["db_name"],
        port=pg_credentials["port"],
        host=pg_credentials["host"],
        user=pg_credentials["user"],
        password=pg_credentials["password"],
        query = """
            CREATE SCHEMA IF NOT EXISTS md;
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE SCHEMA IF NOT EXISTS ods;
            CREATE SCHEMA IF NOT EXISTS dds;
        """
    )