from typing import Any


def transform_nested_fields_for_md_currency(data: dict[str, Any] | None, sending_flag: bool = True) -> dict[str, Any] | None:
    """
    Возвращает словарь, который содержит информацию для загрузки таблицы md.currencies.

    :param sending_flag: Получаем либо валюту отправления, либо валюту получателя.
    :param data: Результат работы API.
    :return: Словарь с информацией о валюте.
    """
    if data is None or data == {}:
        raise TypeError(f"Data could not be None or Empty")
    if not isinstance(data, dict):
        raise TypeError(f"Expected a dict but got {type(data)}")

    if sending_flag:
        currency = "sendingCurrency"
    else:
        currency = "receivingCurrency"
    try:
        curr = data[currency]
        result = {
            "id": int(curr["id"]),
            "code": curr["code"],
            "name": curr["name"]
        }
        return result
    except KeyError as e:
        raise KeyError(f"Ошибка в получении данных валюты: {e}") from e


def transform_nested_fields_for_raw_korona_transfer_rates(data: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Возвращает словарь, который содержит информацию для загрузки таблицы raw.korona_transfer_rates.

    :param data: Результат работы API.
    :return: Словарь с информацией о курсе перевода.
    """
    if data is None or data == {}:
        raise TypeError(f"Data could not be None or Empty")
    if not isinstance(data, dict):
        raise TypeError(f"Expected a dict but got {type(data)}")

    try:
        result = {
            "sending_currency_id": int(data["sendingCurrency"]["id"]),
            "receiving_currency_id": int(data["receivingCurrency"]["id"]),
            "exchange_rate": float(data["exchangeRate"])
        }
        return result
    except KeyError as e:
        raise KeyError(f"Ошибка в получении данных валюты: {e}") from e