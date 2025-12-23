import logging

from dags.utils.utils_koronapay_api import KoronaPayApi

# Конфигурация логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def main():
    kr = KoronaPayApi()
    result = kr.get_data_from_api()
    logging.info(result)


if __name__ == "__main__":
    main()