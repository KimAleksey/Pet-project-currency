import logging

from sqlalchemy import create_engine, text

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def execute_custom_query(
        db_name: str | None = "demo",
        host: str = "localhost",
        user: str = "postgres",
        password: str = "postgres",
        port: int = 5433,
        query: str | None = None,
) -> None:
    """
    Выполняет произвольный запрос в PostgreSQL.

    :param db_name: Имя БД. По-умолчанию demo - из Docker-compose.yaml
    :param host: Хост БД. По-умолчанию localhost
    :param user: Пользователь БД. По-умолчанию postgres - из Docker-compose.yaml
    :param password: Пароль пользователя БД. По-умолчанию postgres - из Docker-compose.yaml
    :param port: Порт БД. По-умолчанию 5433 - из Docker-compose.yaml
    :param query: Пользователь БД. По-умолчанию postgres - из Docker-compose.yaml
    :return: None
    """

    # Подключаемся к БД PostgreSQL
    engine = create_engine(url=f"postgresql://{user}:{password}@{host}:{port}/{db_name}", isolation_level="AUTOCOMMIT")

    # Выполняем пользовательский запрос
    with engine.connect() as connection:
        connection.execute(text(query))
        logging.info(f"👌 Запрос успешно выполнен: {query}")
    engine.dispose()