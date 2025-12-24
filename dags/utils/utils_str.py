def generate_insert_into_for_row(
        schema: str | None = "public",
        table: str | None = None,
        columns: str | None = None,
        placeholders: str | None = None,
        on_conflict_option: str | None = None,
) -> str:
    """
    Функция, которая генерирует строку для генерации запроса INSERT в Postgres

    :param schema: Схема БД
    :param table: Таблица БД
    :param columns: Колонки для вставки
    :param placeholders: Колонки обернутые в плейсхолдеры.
    :return: Str - сгенерированный запрос на INSERT.
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

    return f'INSERT INTO "{schema}"."{table}" ({columns}) VALUES ({placeholders})'


def generate_on_conflict(
        keys: list[str] | None = None,
        attributes: list[str] | None = None,
        on_conflict_option: str | None = "nothing",
) -> str:
    """
    Формирует строку для описания действия при попытке вставки данных с одинаковым ключом.

    :param keys: Список ключей таблицы.
    :param attributes: Список значений, которые нужно переопределить.
    :param on_conflict_option: Что делать, по-умолчанию 'nothing'. Есть опция update, nothing.
    :return: Строка с описанием действия.
    """
    CONFLICT_OPTIONS = ("nothing", "update")
    if not keys and on_conflict_option == "update":
        raise ValueError("Keys are required")
    if not attributes and on_conflict_option == "update":
        raise ValueError("Attributes are required for update")
    if not on_conflict_option in CONFLICT_OPTIONS:
        raise ValueError(f"on_conflict_option must be one of {CONFLICT_OPTIONS}")
    on_conflict = f"ON CONFLICT ({', '.join(keys)}) "
    if on_conflict_option == "nothing":
        option = "DO NOTHING "
        set_ = ""
    else:
        option = "DO UPDATE "
        set_ = f"SET {', '.join([f'{attr}=EXCLUDED.{attr}'for attr in attributes])}"
    result = on_conflict + option + set_
    return result