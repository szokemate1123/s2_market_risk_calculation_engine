import os
import argparse
import requests
from bs4 import BeautifulSoup
import urllib.request
import pandas as pd
import xlwings as xw
from zipfile import ZipFile
from base_functions import dataframe_from_xlsx, recalculate_file_with_xlwings, transpose_sheets
import warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning # type: ignore
import logging

warnings.simplefilter('ignore', InsecureRequestWarning)

# Logging beállítása
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FileFetcher:
    def __init__(self, input_file: str, output_file: str, url: str, css_selector: str, base_url: str, attribute='href', need_day=False, month_format='long', include_year=True, include_month=True, direct_url=None):
        self.input_file = input_file
        self.output_file = output_file
        self.url = url
        self.css_selector = css_selector
        self.base_url = base_url
        self.attribute = attribute
        self.need_day = need_day
        self.month_format = month_format
        self.include_year = include_year
        self.include_month = include_month
        self.direct_url = direct_url

    def ensure_file_exists(self):
        """
        Ha nem létezik a symmetric adjustment file, akkor létrehozzuk üresen, hogy tudjunk mibe menteni.

        Args:
            None
        """
        if not os.path.exists(self.output_file):
            # Csinálunk egy új excel file-t, ha nincsen egy üres Dataframe segítségével
            empty_df = pd.DataFrame()
            empty_df.to_excel(self.output_file, index=False)

    def extract_date(self, df):
        """
        A bemeneti excel file-ban van egy QRT és YEAR érték, ezeket akarjuk beolvasni.

        Args:
            df (pd.DataFrame): DataFrame amiben benne van az év és hónap.

        Returns:
            tuple: A kinyert év (str), hónap (str), és nap (str) ha szükséges.
        """
        if df.empty or df.shape[0] < 2:
            raise ValueError("A dataframe-nek legalább két értéket tartalmaznia kell, a hónapot és az évet.")
        
        year = str(df.iloc[0, 0]) if pd.notnull(df.iloc[0, 0]) else None
        month = str(df.iloc[1, 0]) if pd.notnull(df.iloc[1, 0]) else None
        
        if self.need_day:
            day_mapping = {
                "3": "31",
                "6": "30",
                "9": "30",
                "12": "31"
            }
            if month not in day_mapping:
                raise ValueError(f"Nem jó a hónapnak a száma: {month}. Csak a következők közül lehetnek: 3, 6, 9, 12.")
            day = day_mapping[month]
            return year, month, day
        
        return year, month

    def map_month_to_quarter(self, month):
        """
        A kapott hónap átalakítása megfelelő értékké, hogy le tudjunk tölteni fileokat az EIOPA-ról.
        
        Args:
            month (str): A hónap értéke.

        Returns:
            str: Hónaphoz számértékéhez megfelelő string érték.

        Raises:
            ValueError: Ha nem megfelelő számú a hónap.
        """
        month_mapping_long = {
            "3": "march",
            "6": "june",
            "9": "september",
            "12": "december"
        }
        month_mapping_short = {
            "3": "03",
            "6": "06",
            "9": "09",
            "12": "12"
        }
        if self.month_format == 'short':
            month_mapping = month_mapping_short
        else:
            month_mapping = month_mapping_long
        
        if month not in month_mapping:
            raise ValueError(f"Nem jó a hónapnak a száma: {month}. Csak a következők közül lehetnek: 3, 6, 9, 12.")
        return month_mapping[month]

    def fetch_links(self):
        """
        Letöltjük a file-t az adott honlapról a megadott CSS selector alapján.

        Returns:
            list: A list of attribute values based on the CSS selector.
        """
        response = requests.get(self.url, verify=False)
        if response.status_code != 200:
            raise ConnectionError(f"Nem sikerült letölteni az adatot innen: {self.url}. Státusz kód: {response.status_code}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        selected = soup.select(self.css_selector)
        return [element.get(self.attribute) for element in selected]

    def find_document_path(self, attribute_values, year, month_name, day=None, additional_search_values=None):
        """
        Ez a függvény fogja megkeresni egy adott időszakhoz tartozó pontos honlap linket.

        Args:
            attribute_values (list): List of attribute values.
            year (str): Year string.
            month_name (str): Month name.
            day (str, optional): Day string if needed.
            additional_search_values (list, optional): Additional search values to include in the search.

        Returns:
            str: Ahol elérhető a file.

        Raises:
            FileNotFoundError: Ha nincs ilyen dokumentum az oldalon.
        """
        for value in attribute_values:
            if (year is None or year in value) and (month_name is None or month_name in value.lower()) and (day is None or day in value):
                if additional_search_values:
                    if all(search_value in value for search_value in additional_search_values):
                        return value
                else:
                    return value
        raise FileNotFoundError(f"Nincs ilyen dokumentum ehhez az évhez és hónaphoz {year} {month_name}.")

    def download_file(self, file_link):
        """
        A korábban meghatározott linken lévő file letöltése az input_df-ben található helyre.

        Args:
            file_link (str): URL to the file.
        """
        try:
            urllib.request.urlretrieve(file_link, self.output_file)
            logger.info(f"File downloaded from {file_link} to {self.output_file}")
        except Exception as e:
            raise IOError(f"Nem tudtam letölteni a filet: {file_link}. Error: {e}")

    def process_file(self):
        """
        Ez a függvény kapcsolja össze az eddigieket, végzi el a file letöltését és érték kiolvasását.

        Returns:
            str: Path to the downloaded file.
        """
        # Megnézzük létezik-e a file és hogy excel típusú-e
        if self.output_file.endswith('.xlsx'):
            self.ensure_file_exists()

        df = dataframe_from_xlsx(self.input_file, "calculating_period", header=False)
        date_parts = self.extract_date(df)
        year, month = date_parts[0], date_parts[1]
        month_name = self.map_month_to_quarter(month) if month else None
        day = date_parts[2] if self.need_day else None

        #Itt kell változtatni ha más direct linkű dolgot is le akarunk tölteni
        if self.direct_url == 'mnb':
            # Konstrukció a letöltési URL-hez
            file_link = construct_mnb_url(year)
            self.download_file(f'{self.base_url}{file_link}')
        else:
            # Korábbi dataframe_from_xlsx függvényt használjuk, hogy beolvassuk a szükséges paramétereket
            attribute_values = self.fetch_links()
            document_path = self.find_document_path(attribute_values, year if self.include_year else None, month_name if self.include_month else None, day)
            file_link = f'{self.base_url}{document_path}'
            self.download_file(file_link)
        
        return self.output_file, year, month_name, day

    def run_process(self):
        """
        Wrapper function csak azért, hogy tudjuk futtatni a függvényt esetleg notebookból vagy másik kódból.

        Returns:
            str: Path to the downloaded file.
        """
        return self.process_file()

def extract_symmetric_adjustment(file_path: str):
    """
    Kiszedjük a pontos symmetric adjustment értéket az excelből.

    Args:
        file_path (str): excel file elérési helye.

    Returns:
        float: Symmetric adjustment értéke.

    Raises:
        IOError: Ha nem lehet kiszedni az értéket.
    """
    app = None
    try:
        app = xw.App(visible=False, add_book=False)
        wb = app.books.open(file_path, update_links=False)
        ws = wb.sheets['Symmetric_adjustment']
        sym_adj = ws.range('K8').value
        wb.close()
        return sym_adj
    except Exception as e:
        raise IOError(f"Nem sikerült kiszedni symmetric adjustment-et {file_path}. Error: {e}")
    finally:
        if app:
            app.quit()

def extract_rfr_rates(file_path, year, month_name, day):
    """
    RFR ráta letöltése, a szükséges file kicsomagolása a ZIP-ből, újrakalkulálás és a fülek transzponálása.

    Args:
        file_path (str): Az elmentett ZIP file elérési útja.
        year, month_name, day (str): Dátum, hogy mikori
    """
    # A file elérési útjának és nevének meghatározása
    working_directory = os.path.split(file_path)[0]
    needed_file_name = f'EIOPA_RFR_{year}{month_name}{day}_Term_Structures.xlsx'
    needed_file_path = os.path.join(working_directory, needed_file_name)
    
    # ZIP kicsomagolása
    with ZipFile(file_path, 'r') as zObject:
        zObject.extract(needed_file_name, path=working_directory)
    zObject.close()
    
    # Új file elérési útja
    output_file_path = os.path.join(working_directory, f'{year}_{month_name}_RFR_transposed.xlsx')
    
    recalculate_file_with_xlwings(needed_file_path)
    
    transpose_sheets(needed_file_path, ['RFR_spot_no_VA', 'Spot_NO_VA_shock_UP', 'Spot_NO_VA_shock_DOWN'], output_file_path)
    logger.info(f"RFR rates extracted and processed. Output saved to: {output_file_path}")
    return output_file_path

def filter_xlsx_by_date(file_path, year, month):
    """
    Megnyit egy xlsx file-t, és kiszűri az adott év és hónap alapján az adatokat.
    Az első két sor ID-ként megmarad.

    Args:
        file_path (str): Az input Excel file elérési útja.
        year (str): Az év, amely alapján szűrni kell.
        month (str): A hónap, amely alapján szűrni kell.
    """
    try:
        #Az ID-kat és egységeket külön olvassuk be
        ids = dataframe_from_xlsx(file_path, 'Árfolyamok!A1:CA2', header = 0)

        # Az adatokat DataFrame-be olvassuk, külön az exchange rate-k
        df = dataframe_from_xlsx(file_path, 'Árfolyamok!A3:CA30000')
        df.columns = ids.columns

        # Szűrés az adott év és hónap alapján, figyelembe véve a None értékeket
        df_filtered = df[df.iloc[:, 0].apply(
            lambda x: x.strftime('%Y.%m') == f"{year}.{month}" if pd.notnull(x) else False
            )].copy()
        
        df_filtered.iloc[:, 0] = df_filtered.iloc[:, 0].apply(lambda x: x.strftime('%Y.%m.%d') if pd.notnull(x) else x).copy()
        
        # Az ID-k hozzáadása a szűrt DataFrame-hez
        df_result = pd.concat([ids, df_filtered], axis = 0)

        # Az eredmény mentése egy új Excel file-ba
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df_result.to_excel(writer, index=False, header=False, sheet_name = 'Árfolyamok')

        logger.info(f"Filtered data saved to {file_path}")

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        raise

def construct_mnb_url(year):
    """
    Konstrukció a letöltési URL-hez az ismert minta alapján.

    Args:
        year (str): Az év, amely alapján a letöltési URL-t konstruáljuk.

    Returns:
        str: A letöltési URL.
    """
    return f"/arfolyam-letoltes?year={year}"


if __name__ == "__main__":
    # Ha esetleg akarnánk külön is futtatni
    parser = argparse.ArgumentParser(description="Process symmetric adjustment for a given input file.")
    parser.add_argument("--input-file", required=True, help="Path to the Excel file containing year and month.")
    parser.add_argument("--output-file", required=True, help="Path to save the downloaded Excel file.")
    parser.add_argument("--url", required=True, help="URL of the website to fetch links from.")
    parser.add_argument("--css-selector", required=True, help="CSS selector to find the elements.")
    parser.add_argument("--base-url", required=True, help="Base URL to construct the full file link.")
    parser.add_argument("--attribute", default='href', help="Attribute to extract from the elements (default is 'href').")
    parser.add_argument("--extract-symmetric-adjustment", action='store_true', help="Flag to extract symmetric adjustment from the downloaded file.")
    parser.add_argument("--need-day", action='store_true', help="Flag to indicate if the day is needed in the date extraction.")
    parser.add_argument("--month-format", choices=['long', 'short'], default='long', help="Format of the month (default is 'long').")
    parser.add_argument("--include-year", action='store_true', help="Flag to include the year in the link construction.")
    parser.add_argument("--include-month", action='store_true', help="Flag to include the month in the link construction.")
    parser.add_argument("--additional-search-values", nargs='*', help="Additional search values to include in the search.")

    args = parser.parse_args()
    input_file = args.input_file
    output_file = args.output_file
    url = args.url
    css_selector = args.css_selector
    base_url = args.base_url
    attribute = args.attribute
    extract_symmetric_adjustment_flag = args.extract_symmetric_adjustment
    need_day = args.need_day
    month_format = args.month_format
    include_year = args.include_year
    include_month = args.include_month
    additional_search_values = args.additional_search_values

    processor = FileFetcher(input_file, output_file, url, css_selector, base_url, attribute, need_day, month_format, include_year, include_month)
    try:
        downloaded_file = processor.run_process()
        if extract_symmetric_adjustment_flag:
            sym_adj_value = extract_symmetric_adjustment(downloaded_file)
            print(f"Symmetric Adjustment Value: {sym_adj_value}")
        else:
            print(f"File downloaded to: {downloaded_file}")
    except Exception as e:
        logger.error(f"Error: {e}")
        print(f"Error: {e}")