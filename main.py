import logging

from utils.utils_koronapay_api import KoronaPayApi
from utils.utils_dict import generate_fields_from_dict
from utils.utils_dict import generate_fields_from_dict_in_placeholder
from utils.utils_transform import transform_nested_fields_for_md_currency
from utils.utils_transform import transform_nested_fields_for_raw_korona_transfer_rates
from utils.utils_str import generate_insert_into_for_row
from utils.utils_str import generate_on_conflict
from utils.utils_load_to_postgres import save_dict_to_postgres

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
    save_dict_to_postgres(
        conn_id="my_dwh",
        schema="md",
        table="currencies",
        dict_row=md,
        keys=["id"],
        attributes=["code", "name"],
        on_conflict_option="update"
    )

    raw = transform_nested_fields_for_raw_korona_transfer_rates(api_data)
    save_dict_to_postgres(
        conn_id="my_dwh",
        schema="raw",
        table="korona_transfer_rates",
        dict_row=raw,
    )




if __name__ == "__main__":
    main()