import logging
from file_fetcher_class import FileFetcher, extract_rfr_rates, filter_xlsx_by_date
from base_functions import dataframe_from_xlsx
import pandas as pd

# Logging beállítása
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('file_fetching.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

def run_all_fetching(input_file_path):
    """
    Az összes folyamat futtatása a megadott input fájl útvonallal.

    Args:
        input_file_path (str): Az input Excel fájl útvonala.
    """
    input_df = dataframe_from_xlsx(input_file_path, "input_df", header=True)
    date_df = dataframe_from_xlsx(input_file_path, "calculating_period", header=False)

    input_year = str(date_df.iloc[0, 0]) if pd.notnull(date_df.iloc[0, 0]) else None
    input_month = str(date_df.iloc[1, 0]) if pd.notnull(date_df.iloc[1, 0]) else None
    input_month = f'0{input_month}'

    try:
        sym_adj_filename = input_df.loc[input_df["file_name"] == "sym_adj", "file_path"].values[0]
    except IndexError:
        logger.error("Nincs bejegyzés a 'sym_adj' számára az input fájlban.")
        sym_adj_filename = None

    try:
        extr_rfr_filename = input_df.loc[input_df["file_name"] == "rfr_rates", "file_path"].values[0]
    except IndexError:
        logger.error("Nincs bejegyzés az 'rfr_rates' számára az input fájlban.")
        extr_rfr_filename = None

    try:
        ex_rates_filename = input_df.loc[input_df["file_name"] == "exchange_rates", "file_path"].values[0]
    except IndexError:
        logger.error("Nincs bejegyzés az 'exchange_rates' számára az input fájlban.")
        ex_rates_filename = None

    sym_adj_args = [
        input_file_path,
        sym_adj_filename,
        'https://www.eiopa.europa.eu/tools-and-data/symmetric-adjustment-equity-capital-charge_en',
        '.ecl-file__download',
        'https://www.eiopa.europa.eu',
        'href',
        False,
        'long',
        True,
        True
    ]

    extr_rfr_args = [
        input_file_path,
        extr_rfr_filename,
        'https://www.eiopa.europa.eu/tools-and-data/risk-free-interest-rate-term-structures_en',
        '.ecl-file__download',
        'https://www.eiopa.europa.eu',
        'href',
        True,
        'short',
        True,
        True
    ]

    ex_rates_args = [
        input_file_path,
        ex_rates_filename,
        'https://www.mnb.hu/arfolyam-lekerdezes',
        '/arfolyam-letoltes',
        'https://www.mnb.hu',
        'action',
        False,
        'short',
        False,
        False,
        'mnb'
    ]

    args_list = [sym_adj_args, extr_rfr_args, ex_rates_args]

    rfr_rates_file_path = None

    # Letöltünk minden file-t, ha hiba van, akkor 5x újrapróbálja
    for args in args_list:
        retries = 5
        for attempt in range(retries):
            try:
                processor = FileFetcher(*args)
                downloaded_file, year, month_name, day = processor.run_process()
                if 'risk-free-interest-rate-term-structures' in args[2]:
                    rfr_rates_file_path = extract_rfr_rates(downloaded_file, year, month_name, day)
                elif 'arfolyam-lekerdezes' in args[2]:
                    filter_xlsx_by_date(downloaded_file, input_year, input_month)
                break  # Ha sikeres, akkor lépjünk ki a for ciklusból
            except Exception as e:
                logger.error(f"Hiba a feldolgozás során az alábbi paraméterekkel {args} a(z) {attempt + 1}. próbálkozásnál: {e}")
                if attempt == retries - 1:
                    logger.error(f"Sikertelen feldolgozás az alábbi paraméterekkel {args} {retries} próbálkozás után")

    return rfr_rates_file_path

if __name__ == "__main__":
    input_file_path = "../market_risk/input_to_calculate.xlsx"
    run_all_fetching(input_file_path)