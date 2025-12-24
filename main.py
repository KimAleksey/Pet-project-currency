import logging

from utils.utils_koronapay_api import KoronaPayApi
from utils.utils_dict import generate_fields_from_dict
from utils.utils_dict import generate_fields_from_dict_in_placeholder
from utils.utils_transform import transform_nested_fields_for_md_currency
from utils.utils_transform import transform_nested_fields_for_raw_korona_transfer_rates
from utils.utils_str import generate_insert_into_for_row
from utils.utils_str import generate_on_conflict

# Конфигурация логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def main():
    # kr = KoronaPayApi()
    # result = kr.get_data_from_api()
    # logging.info(result)

    api_data = {
        'sendingCurrency': 
            {'id': '810', 'code': 'RUB', 'name': 'Российский рубль'}, 
        'sendingAmount': 831327, 
        'sendingAmountDiscount': 0, 
        'sendingAmountWithoutCommission': 831327, 
        'sendingCommission': 0, 
        'sendingCommissionDiscount': 0, 
        'sendingTransferCommission': 0, 
        'paidNotificationCommission': 0, 
        'receivingCurrency': 
            {'id': '840', 'code': 'USD', 'name': 'Доллар США'}, 
        'receivingAmount': 10000, 
        'exchangeRate': 83.1327, 
        'exchangeRateType': 'direct', 
        'exchangeRateDiscount': 0, 
        'profit': 0, 
        'properties': {}
    }

    md = transform_nested_fields_for_md_currency(api_data)
    md_columns = generate_fields_from_dict(md)
    md_placeholders = generate_fields_from_dict_in_placeholder(md)
    md_insert = generate_insert_into_for_row(
        schema="md",
        table="currencies",
        columns=md_columns,
        placeholders=md_placeholders
    )
    md_on_conflict = generate_on_conflict(
        keys=["id"],
        attributes=["code", "name"],
        on_conflict_option="nothing"
    )
    mn_sql = md_insert + " " + md_on_conflict
    print(mn_sql)

    raw = transform_nested_fields_for_raw_korona_transfer_rates(api_data)
    raw_columns = generate_fields_from_dict(raw)
    raw_placeholders = generate_fields_from_dict_in_placeholder(raw)
    raw_insert = generate_insert_into_for_row(
        schema="raw",
        table="korona_transfer_rates",
        columns=raw_columns,
        placeholders=raw_placeholders
    )
    raw_on_conflict = generate_on_conflict(
        keys=["sending_currency_id", "receiving_currency_id"],
        attributes=["exchange_rate"],
        on_conflict_option="update"
    )
    raw_sql = raw_insert + " " + raw_on_conflict
    print(raw_sql)

if __name__ == "__main__":
    main()