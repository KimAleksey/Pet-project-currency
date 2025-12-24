from typing import Any

def generate_fields_from_dict(dict_row: dict[str, Any] | None) -> str:
    """
    Генерирует строку с колонками для вставки.

    :param dict_row: Словарь, ключи которого являются колонками для вставки в таблицу.
    :return: Str, состоящий из списка полей для вставки в таблицу.
    """
    if not dict_row:
        return ""
    if not isinstance(dict_row, dict):
        raise TypeError("dict_row must be a dict")
    return ", ".join(dict_row.keys())


def generate_fields_from_dict_in_placeholder(dict_row: dict[str, Any] | None) -> str:
    """
    Функция формирует строку, в которой каждое поле обернуто в placeholder.

    :param dict_row:
    :return:
    """
    if not dict_row:
        return ""
    if not isinstance(dict_row, dict):
        raise TypeError("dict_row must be a dict")
    return ", ".join([f"%({k})s" for k in dict_row.keys()])