def generate_insert_into_for_row(
        schema: str | None = "public",
        table: str | None = None,
        columns: str | None = None,
        placeholders: str | None = None,
) -> str:
    """
    Функция, которая генерирует строку для генерации запроса INSERT в Postgres

    :param schema: Схема БД
    :param table: Таблица БД
    :param columns: Колонки для вставки
    :param placeholders: Колонки обернутые в плейсхолдеры
    :return: str - сгенерированный запрос на INSERT
    """
    if not table:
        raise ValueError("Table is required")

    if not columns:
        raise ValueError("Columns are required")

    if not placeholders:
        raise ValueError("Placeholders are required")

    if schema is not None and not isinstance(schema, str):
        raise TypeError("Schema must be a string")

    if table is not None and not isinstance(table, str):
        raise TypeError("Table must be a string")

    if columns is not None and not isinstance(columns, str):
        raise TypeError("Columns must be a string")

    if placeholders is not None and not isinstance(placeholders, str):
        raise TypeError("Placeholders must be a string")

    return f'INSERT INTO "{schema}"."{table}" ({columns}) VALUES ({placeholders});'