from typing import Any


def transform_nested_fields(data: dict[str, list[dict]] | None ) -> dict[str, Any] | None:
    """
    Возвращает словарь, который содержит информацию по одному пользователю.

    :param data: Результат работы API.
    :return: Словарь с информацией об одном пользователе.
    """
    if data is None or data == {}:
        raise TypeError(f"Data could not be None or Empty")
    if not isinstance(data, dict):
        raise TypeError(f"Expected a dict but got {type(data)}")

    try:
        user_data_list = data["results"]
    except KeyError:
        raise KeyError(f"Data should have key 'results'. {data}")

    try:
        user_data = user_data_list[0]
    except IndexError:
        raise IndexError(f"Data should have non empty list in dictionary with key 'results'. {data}")

    try:
        login = user_data["login"]
        name = user_data["name"]
        location = user_data["location"]

        result = {
            "username": login["username"],
            "password": login["password"],
            "firstname": name["first"],
            "lastname": name["last"],
            "email": user_data["email"],
            "country": location["country"],
            "city": location["city"]
        }
    except KeyError as e:
        raise KeyError(f"User's data should have: {e}") from e

    return result