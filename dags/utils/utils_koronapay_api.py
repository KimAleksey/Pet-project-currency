import urllib.parse
import logging
import pycountry
import duckdb
import requests

from pathlib import Path

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

class CountryUtils:
    @staticmethod
    def check_country_is_valid(country: str | None = None) -> bool:
        """
        Проверка на валидность вводимой страны.
        Например, для России валидные коды RUS, RU, Russian Federation, 643

        :param country: Страна в виде строки.
        :return: True, если страна найдена, иначе False.
        """
        if not country:
            logging.info("Не задана страна для проверки.")
            return False
        if not isinstance(country, str):
            raise TypeError("Страна должна иметь тип str.")
        logging.info(f"Проверка страны на {country} валидность.")
        try:
            country_info = pycountry.countries.lookup(country)
            logging.info(f"Заданная страна: {country_info}")
        except LookupError:
            logging.info(f"Заданная страна не найдена: {country}.")
            return False
        return True

    @staticmethod
    def get_county_code(country: str | None = None) -> str | None:
        """
        Возвращает код страны, валидный для API KoronaPay.
        Например, для России - RUS

        :param country: Страна в виде строки.
        :return: Код страны в формате 3-х символьной строки (RUS).
        """
        logging.info(f"Получаем код для страны: {country}.")
        if not KoronaPayUtils.check_country_is_valid(country):
            return None
        try:
            country_code = pycountry.countries.lookup(country).alpha_3
            logging.info(f"Код страны: {country_code}")
        except Exception:
            raise RuntimeError(f"Для страны {country} не известен 3-х символьный код.")
        return country_code


class CurrencyUtils:
    FILE_PATH = Path(__file__).parent / "data" / "codes-all.csv"

    @staticmethod
    def check_currency_code_is_valid_iso_4217(currency: str | None = None) -> bool:
        """
        Проверка, что код валюты валиден согласно стандарту ISO 4217.

        :param currency: Код валюты, в формате строки. Например, "840".
        :return: True, если код валюты найден, иначе False.
        """
        if not currency:
            logging.info("Нет валюты для проверки.")
            return False
        if not isinstance(currency, str):
            raise TypeError(f"Валюта должна быть строкой, а не: {type(currency).__name__}.")
        try:
            res = duckdb.sql(f"SELECT * FROM '{CurrencyUtils.FILE_PATH}' WHERE NumericCode = '{currency}'").fetchall()
            if len(res) != 0:
                return True
            return False
        except Exception as e:
            raise RuntimeError(f"Не получилось получить данные из файла и проверить наличие валюты. {e}")

    @staticmethod
    def check_currency_code_is_valid(currency: str | None = None) -> bool:
        """
        Проверяем валидность кода валюты.
        Проверка в два этапа, сначала в библиотеке pycountry, если код там не найден,
        то поиск дополнительно производится в ISO 4217.

        :param currency: Код валюты, в формате строки. Например, "840".
        :return: True, если код валюты найден, иначе False.
        """
        if not currency:
            logging.info("Нет валюты для проверки.")
            return False
        if not isinstance(currency, str):
            raise TypeError(f"Валюта должна быть строкой, а не: {type(currency).__name__}.")
        # Проверка в библиотеке pycountry
        curr_code = next((c for c in pycountry.currencies if c.numeric == currency), None)
        if not curr_code is None:
            return True
        # Проверка в ISO 4217
        return CurrencyUtils.check_currency_code_is_valid_iso_4217(currency)


class KoronaPayUtils:
    PAYMENT_METHODS = ("debitCard")
    RECEIVING_METHODS = ("cash")

    @staticmethod
    def check_payment_receiving_method_is_valid(payment: bool, method: str | None = None) -> bool:
        """
        Проверяем доступные методы оплаты/получения.
        При возникновении новых - добавить в PAYMENT_METHODS и RECEIVING_METHODS текущего класса.

        :param payment: Если True, то проверяем платежный метод, иначе метод получения.
        :param method: Метод оплаты в виде строки.
        :return: True если валидный, иначе False.
        """
        if not isinstance(method, str):
            raise TypeError("Метод должен иметь тип str.")
            return False
        if not isinstance(payment, bool):
            raise TypeError("payment должен быть типом bool.")
        if payment:
            logging.info(f"Проверка метода оплаты на {method} валидность.")
            return method in KoronaPayUtils.PAYMENT_METHODS
        else:
            logging.info(f"Проверка метода получения на {method} валидность.")
            return method in KoronaPayUtils.RECEIVING_METHODS

    @staticmethod
    def check_paid_notification_method_is_valid(flag: bool | None = None) -> bool:
        """
        Проверка флага уведомления.

        :param flag: bool
        :return: bool
        """
        if not isinstance(flag, bool):
            raise TypeError(f"Флаг уведомления должен быть bool, а не {type(flag).__name__}")
        return True

    @staticmethod
    def check_receiving_amount_is_valid(amount: int | None = None) -> bool:
        """
        Проверка суммы получения. Должна быть больше 5.

        :param amount: int, Сумма для перевода
        :return: bool, True если int и > 5, иначе False.
        """
        if amount is None:
            logging.info("Сумма не передана")
            return False
        if not isinstance(amount, int):
            raise TypeError(f"Флаг уведомления должен быть bool, а не {type(amount).__name__}")
        if amount <= 5:
            logging.info("Сумма должна быть больше 5")
            return False
        return True

class KoronaPayApi:
    BASE_URL = "https://api.koronapay.com/transfers/tariffs"

    def __init__(
            self,
            sending_country_id: str | None = "RUS",
            sending_currency_id: str | None = "810",
            receiving_country_id: str | None = "GEO",
            receiving_currency_id: str | None = "840",
            payment_method: str | None = "debitCard",
            receiving_method: str | None = "cash",
            paid_notification_enabled: bool | None = False,
            receiving_amount: int | None = 100,
    ):
        """
        Создается экземпляр обращения к API Золотой короны.

        :param sending_country_id: Код страны отправителя, RUS
        :param sending_currency_id: Код валюты отправителя, 810
        :param receiving_country_id: Код страны получателя, GEO
        :param receiving_currency_id: Код валюты получателя, 840
        :param payment_method: Метод оплаты, debitCard
        :param receiving_method: Метод получения, cash
        :param paid_notification_enabled: Уведомление о переводе, true
        :param receiving_amount: Сумма к отправке
        """
        if not CountryUtils.check_country_is_valid(sending_country_id):
            raise ValueError(f"Недействительное значение sending_country_id: {sending_country_id}")

        if not CurrencyUtils.check_currency_code_is_valid(sending_currency_id):
            raise ValueError(f"Недействительное значение sending_currency_id: {sending_currency_id}")

        if not CountryUtils.check_country_is_valid(receiving_country_id):
            raise ValueError(f"Недействительное значение receiving_country_id: {receiving_country_id}")

        if not CurrencyUtils.check_currency_code_is_valid(receiving_currency_id):
            raise ValueError(f"Недействительное значение receiving_currency_id: {receiving_currency_id}")

        if not KoronaPayUtils.check_payment_receiving_method_is_valid(payment=True, method=payment_method):
            raise ValueError(f"Недействительный способ оплаты: {payment_method}")

        if not KoronaPayUtils.check_payment_receiving_method_is_valid(payment=False, method=receiving_method):
            raise ValueError(f"Недействительный способ получения: {payment_method}")

        if not KoronaPayUtils.check_paid_notification_method_is_valid(paid_notification_enabled):
            raise ValueError(f"Недопустимое значение paid_notification_enabled: {paid_notification_enabled}")

        if not KoronaPayUtils.check_receiving_amount_is_valid(receiving_amount):
            raise ValueError(f"Недопустимая сумма получения: {receiving_amount}")

        self._params = {
            "sendingCountryId": sending_country_id,
            "sendingCurrencyId": sending_currency_id,
            "receivingCountryId": receiving_country_id,
            "receivingCurrencyId": receiving_currency_id,
            "paymentMethod": payment_method,
            "receivingMethod": receiving_method,
            "paidNotificationEnabled": str(paid_notification_enabled).lower(),
            "receivingAmount": str(receiving_amount * 100),
        }

        self._headers = {
            "User-Agent": "Mozilla/5.0"
        }

    def get_api_url(self) -> str:
        """
        Собирает полный URL с параметрами для запроса к API.

        :return: URL для обращения к API.
        """
        query_string = urllib.parse.urlencode(self._params)
        return f"{self.BASE_URL}?{query_string}"

    def get_data_from_api(self) -> dict | None:
        """
        GET запрос к API Золотой короны.

        :return: Ответ, полученный от API в формате JSON
        """
        url = self.get_api_url()
        logging.info(f"Выполнение GET запроса для URL: {url}")
        try:
            response = requests.get(url=url, headers=self._headers, timeout=600)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Ошибка подключения к API. {e}")

        if response.status_code == 200:
            try:
                api_data = response.json()[0]
                logging.info(f"Получены следующие данные: {api_data}")
            except Exception as e:
                raise RuntimeError("Ошибка при формировании JSON.")
        else:
            raise RuntimeError("Не удалось получить ответ")
        logging.info(f"🔥 Данные успешно получены.")
        return api_data