#Importok
from abc import ABC, abstractmethod
import pandas as pd
import logging
from base_functions import process_workbook, remove_illegal_characters
import traceback
from datetime import datetime, timedelta

#Az alap fő class, asset list, ul list és tpt data validáláshoz
class BaseValidator:
    def __init__(self, df, column_types=None):
        """
        Alapértelmezett inicializálás a BaseValidator class-ban.
        
        Args:
        - df (pd.DataFrame): A DataFrame, amely az adatokat tartalmazza.
        - column_types (dict): Szótár, amely az oszlopneveket a várt adattípusokhoz rendeli.
        """
        if df.empty:
            raise ValueError("Üres a bemeneti dataframe.")
        
        # Az első oszlop alapján eltávolítjuk a hiányzó értékeket tartalmazó sorokat
        first_column = df.columns[0]
        df = df.dropna(subset=[first_column]).copy()
        df = df[df[first_column] != ''].copy()

        self.df = remove_illegal_characters(df.copy())
        self.column_types = column_types or {}
        self.validation_errors = []
        self.critical_errors = []

        # Logging beállítása
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        handler = logging.FileHandler('data_validation.log')
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
        self.logger.propagate = False

        # Az oszlopok átalakítása a várt adattípusokra
        if self.column_types:
            self.convert_column_types()

    def convert_maturity_date(self, value):
        try:
            if pd.isna(value):
                return pd.NaT
            if isinstance(value, (int, float)):
                return datetime(1899, 12, 30) + timedelta(days=value)
            else:
                return pd.to_datetime(value, format='%Y.%m.%d', errors='coerce')
        except Exception as e:
            self.logger.error(f"Hiba a dátum konvertálása közben: {e}")
        return pd.NaT

    def convert_column_types(self):
        """
        Az oszlopok átalakítása a várt adattípusokra.
        """
        for col, col_type in self.column_types.items():
            try:
                if col_type == 'datetime64[ns]':
                    # Explicit módon átalakítjuk datetime64[ns] típusra
                    self.df[col] = pd.to_datetime(self.df[col], format='%d.%m.%Y', errors='coerce')
                elif col_type == 'timedelta64[ns]':
                    # Explicit módon átalakítjuk timedelta64[ns] típusra
                    self.df[col] = pd.to_timedelta(self.df[col], format='%d.%m.%Y', errors='coerce')
                elif col_type == int:
                    # Nem véges értékek kezelése az egész szám konverzióhoz
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                    #Átalakítás, maradhatnak üres értékek
                    self.df[col] = self.df[col].astype('Int64')
                else:
                    self.df[col] = self.df[col].astype(col_type)
            except ValueError as e:
                affected_rows = self.df[self.df[col].astype(str).str.contains(',', na=False)]
                for _, row in affected_rows.iterrows():
                    self.validation_errors.append({
                        "Error": f"Az {col} oszlopot nem sikerült átalakítani {col_type} típusra: {e}",
                        "portfolio_id": row['portfolio_id'],
                        "id_of_underlying": row['id_of_underlying']
                    })
                self.logger.error(f"Az {col} oszlopot nem sikerült átalakítani {col_type} típusra: {e}")

    def save_validation_report(self, validation_report, critical_report, file_path, range_names={'Validation Errors': 'validation_errors', 'Critical Errors': 'critical_errors'}):
        """
        A validációs jelentés mentése egy Excel fájlba.
        
        Args:
        - validation_report (pd.DataFrame): A validációs hibákat tartalmazó DataFrame.
        - critical_report (pd.DataFrame): A kritikus hibákat tartalmazó DataFrame.
        - file_path (str): Az Excel fájl mentési útvonala.
        - range_names (dict): dictionary, amely az egyes sheetek tartományneveit tartalmazza.
        """
        try:
            with pd.ExcelWriter(file_path) as writer:
                validation_report.to_excel(writer, sheet_name='Validation Errors', index=False)
                critical_report.to_excel(writer, sheet_name='Critical Errors', index=False)
        except Exception as e:
            self.logger.error(f"Hiba a validációs jelentés mentése közben: {e}")
            self.logger.error(traceback.format_exc())
        
        process_workbook(file_path, range_names)

    def validate(self):
        """
        Az összes validációs ellenőrzés futtatása.
        """
        raise NotImplementedError("Az alosztályoknak implementálniuk kell ezt a metódust.")

class TPTDataValidator(BaseValidator):
    def __init__(self, df, irr_related_types=None, mandatory_columns=None, specific_columns=None, column_types=None, enable_checks=None):
        """
        Létrehozzuk az alap elemet a TPTDataValidator class-ban.
        
        Args:
        - df (pd.DataFrame): A DataFrame, amely az adatokat tartalmazza.
        - irr_related_types (list): Az IRR-hez kapcsolódó típusok listája.
        - mandatory_columns (list): A kötelező oszlopok listája, amelyeket ellenőrizni kell a hiányzó értékek szempontjából.
        - specific_columns (list): Az egyes validációkhoz használt specifikus oszlopok listája.
        - column_types (dict): Szótár, amely az oszlopneveket a várt adattípusokhoz rendeli.
        - enable_checks (dict): Szótár, amely lehetővé teszi vagy letiltja az egyes ellenőrzéseket.
        """
        super().__init__(df)

        self.column_types = column_types or {
            "portfolio_id": str,
            "valuation_date": 'datetime64[ns]',
            "portfolio_mod_dur": float,
            "scr_delivery": str,
            "cic_of_underlying": str,
            "economic_zone": int,
            "id_of_underlying": str,
            "name_of_underlying": str,
            "qc_of_underlying": str,
            "valuation_weight": float,
            "maturity_date": 'datetime64[ns]',
            "cqs": object,
            "mod_dur_of_underlying": float,
            "underlying_asset_category": str,
        }

        self.enable_checks = enable_checks or {
            "check_valuation_weight_sum": True,
            "check_valuation_weight_for_types": True,
            "check_cqs_for_types": True,
            "check_economic_zone_for_types": True,
            "check_cic_code_vs_type": True,
            "check_scr_delivery": True,
            "check_mandatory_columns": True,
            "check_duplicate_rows": True,
            "check_data_types": True,
            "check_valid_ranges": True,
            "check_portfolio_mod_dur": True,
            "check_irr_related_types": True,
            "check_qc_of_underlying": True
        }

        self.irr_related_types = irr_related_types or [
            '1', '2', '4', '5', '6', '8'
        ]

        self.mandatory_columns = mandatory_columns or [
            "portfolio_id", "valuation_date", "scr_delivery", "cic_of_underlying",
            "id_of_underlying", "qc_of_underlying", "valuation_weight", 
            "portfolio_mod_dur", "underlying_asset_category"
        ]

        self.specific_columns = specific_columns or [
            "portfolio_id", "valuation_date", "portfolio_mod_dur", "scr_delivery", 
            "cic_of_underlying", "economic_zone", "valuation_weight", "cqs", 
            "mod_dur_of_underlying", "underlying_asset_category", "name_of_underlying"
        ]

        # Ellenőrizzük, hogy a DataFrame tartalmazza-e a szükséges oszlopokat
        missing_mandatory = [col for col in self.mandatory_columns if col not in df.columns]
        if missing_mandatory:
            raise ValueError(f"A DataFrame hiányzó kötelező oszlopokat tartalmaz: {missing_mandatory}")
        
        missing_specific = [col for col in self.specific_columns if col not in df.columns]
        if missing_specific:
            raise ValueError(f"A DataFrame hiányzó specifikus oszlopokat tartalmaz: {missing_specific}")

        # A DataFrame előfeldolgozása
        self.preprocess_dataframe()

        # Az oszlopok átalakítása a várt adattípusokra
        self.convert_column_types()

    def preprocess_dataframe(self):
        """
        A DataFrame előfeldolgozása a gyakori adatproblémák kezelésére.
        """
        for col, col_type in self.column_types.items():
            if col_type == float:
                # Vesszők pontokra cserélése a float konverzióhoz
                self.df[col] = self.df[col].astype(str).str.replace(',', '.').astype(float, errors='ignore')
            elif col_type == 'datetime64[ns]':
                # Dátum oszlopok konvertálása a megfelelő formátumra
                self.df[col] = pd.to_datetime(self.df[col], format='%d.%m.%Y', errors='coerce')

    def validate(self):
        """
        Az összes validációs ellenőrzés futtatása.
        """
        self.check_cic_code_vs_type()
        self.check_valuation_weight_sum()
        self.check_valuation_weight_for_types()
        self.check_cqs_for_types()
        self.check_economic_zone_for_types()
        self.check_scr_delivery()
        self.check_mandatory_columns()
        self.check_duplicate_rows()
        self.check_irr_related_types()
        self.check_data_types()
        self.check_portfolio_mod_dur()
        self.check_qc_of_underlying()

        validation_report = pd.DataFrame([
            {
                "Error": error["Error"],
                "portfolio_id": error["portfolio_id"],
                "id_of_underlying": error["id_of_underlying"],
                "cic_code": error.get("cic_code"),
                "type_from_cic_code": error.get("type_from_cic_code"),
                "original_type": error.get("original_type")
            }
            for error in self.validation_errors
        ])

        critical_report = pd.DataFrame([
            {
                "Error": error["Error"],
                "portfolio_id": error["portfolio_id"],
                "id_of_underlying": error["id_of_underlying"]
            }
            for error in self.critical_errors
        ])

        return self.df, validation_report, critical_report

    def check_valuation_weight_sum(self):
        """
        Check if the valuation weight sum is between 0.9999 and 1.0001 for each portfolio_id.
        """
        if not self.enable_checks.get("check_valuation_weight_sum", True):
            return
        try:
            grouped = self.df.groupby(self.specific_columns[0])[self.specific_columns[6]].sum()
            invalid_weights = grouped[(grouped < 0.999) | (grouped > 1.001)].index.tolist()
            if invalid_weights:
                affected_rows = self.df[self.df[self.specific_columns[0]].isin(invalid_weights)]
                for _, row in affected_rows.iterrows():
                    self.critical_errors.append({
                        "Error": "Valuation weight sum is not between 0.999 and 1.001 for some portfolio_id values",
                        "portfolio_id": row['portfolio_id'],
                        "id_of_underlying": row['id_of_underlying']
                    })
                self.logger.warning("Valuation weight sum is not between 0.999 and 1.001 for some portfolio_id values")
        except Exception as e:
            self.logger.error(f"Error in check_valuation_weight_sum: {e}")

    def check_irr_related_types(self):
        """
        Check if the IRR-related types have missing maturity_date and modify calculation_type to "3X".
        """
        if not self.enable_checks.get("check_irr_related_types", True):
            return
        try:
            condition = self.df[self.specific_columns[9]].isin(self.irr_related_types) & self.df['maturity_date'].isnull()
            self.df.loc[condition, 'calculation_type'] = '3X'
        except Exception as e:
            self.logger.error(f"Error in check_irr_related_types: {e}")

    def check_valuation_weight_for_types(self):
        """
        Check if the valuation weight for types A-F exceeds 0.12 for each portfolio_id.
        """
        if not self.enable_checks.get("check_valuation_weight_for_types", True):
            return
        try:
            condition = self.df[self.specific_columns[9]].isin(['A', 'B', 'C', 'D', 'E', 'F'])
            grouped = self.df[condition].groupby(self.specific_columns[0])[self.specific_columns[6]].sum()
            invalid_portfolios = grouped[grouped > 0.12].index.tolist()
            
            # Update calculation_type for invalid portfolios
            self.df.loc[self.df[self.specific_columns[0]].isin(invalid_portfolios), 'calculation_type'] = '3X'
            
            if invalid_portfolios:
                self.logger.warning(f"Valuation weight for types A-F exceeds 0.12 for portfolios: {invalid_portfolios}")
        except Exception as e:
            self.logger.error(f"Error in check_valuation_weight_for_types: {e}")

    def check_cqs_for_types(self):
        """
        Check if the CQS column is empty for underlying_asset_type 1, 2, 8.
        """
        if not self.enable_checks.get("check_cqs_for_types", True):
            return
        try:
            condition = self.df[self.specific_columns[9]].isin(['1', '2', '8'])
            self.df.loc[condition & self.df[self.specific_columns[7]].isnull(), 'calculation_type'] = '3X'
        except Exception as e:
            self.logger.error(f"Error in check_cqs_for_types: {e}")

    def check_economic_zone_for_types(self):
        """
        Check if the economic_zone column is filled for underlying_asset_type "3X" and "3L".
        """
        if not self.enable_checks.get("check_economic_zone_for_types", True):
            return
        try:
            condition = self.df[self.specific_columns[9]].isin(['3X', '3L'])
            self.df.loc[condition & self.df[self.specific_columns[5]].isnull(), 'calculation_type'] = '3X'
        except Exception as e:
            self.logger.error(f"Error in check_economic_zone_for_types: {e}")

    def check_cic_code_vs_type(self):
        """
        Check if the third value of the cic_code_instrument column matches the underlying_asset_type.
        """
        if not self.enable_checks.get("check_cic_code_vs_type", True):
            return
        try:
            cic_codes = self.df[self.specific_columns[4]].astype(str)

            # Handle invalid asset categories
            valid_categories = ['0', '1', '2', '3', '3X', '3L', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']
            invalid_categories = ~self.df[self.specific_columns[9]].astype(str).isin(valid_categories)
            self.df.loc[invalid_categories, 'calculation_type'] = '3X'

            # Check if the CIC code length is 4
            invalid_cic_length = cic_codes.str.len() != 4
            if invalid_cic_length.any():
                affected_rows = self.df[invalid_cic_length]
                for _, row in affected_rows.iterrows():
                    self.validation_errors.append({
                        "Error": "CIC code length is not 4",
                        "portfolio_id": row['portfolio_id'],
                        "id_of_underlying": row['id_of_underlying'],
                        "cic_code": row[self.specific_columns[4]],
                        "type_from_cic_code": None,
                        "original_type": row[self.specific_columns[9]]
                    })
                self.df.loc[invalid_cic_length, 'calculation_type'] = '3X'
                self.logger.warning("CIC code length is not 4 for some rows")

            third_values = cic_codes.str[2]
            expected_types = third_values.where(third_values != '3', self.df[self.specific_columns[5]].map({1: '3L', 2: '3L'}).fillna('3X'))
            mismatches = expected_types != self.df[self.specific_columns[9]]
            affected_rows = self.df[mismatches]
            for _, row in affected_rows.iterrows():
                self.validation_errors.append({
                    "Error": "CIC code and underlying asset type mismatch",
                    "portfolio_id": row['portfolio_id'],
                    "id_of_underlying": row['id_of_underlying'],
                    "cic_code": row[self.specific_columns[4]],
                    "type_from_cic_code": expected_types[row.name],
                    "original_type": row[self.specific_columns[9]]
                })
            self.df.loc[(self.df['calculation_type'] != '3X'), 'calculation_type'] = self.df[self.specific_columns[9]]
            self.df.loc[self.df[self.specific_columns[9]] == 'L', 'calculation_type'] = '7'
            self.df.loc[self.df[self.specific_columns[9]] == '0', 'calculation_type'] = '3X'

            #Még egy ellenőrzés Type 1-re
            condition = (
                (self.df[self.specific_columns[9]] == '1') &
                (~cic_codes.str.startswith('HU') &
                ~self.df[self.specific_columns[10]].str.contains('EIB|HGB|HTB', case=False, na=False))
            )
            self.df.loc[condition, 'calculation_type'] = '1X'

        except Exception as e:
            self.logger.error(f"Error in check_cic_code_vs_type: {e}")

    def check_scr_delivery(self):
        """
        Check if the scr_delivery column value is "N", the portfolio_mod_dur column value is missing, and the underlying_asset_type is 1, 2, 5, 6, 8, D, E, F.
        """
        if not self.enable_checks.get("check_scr_delivery", True):
            return
        try:
            condition = (self.df[self.specific_columns[3]] == 'N') & \
                        (self.df[self.specific_columns[2]].isna()) & \
                        (self.df[self.specific_columns[9]].isin(self.irr_related_types))
            invalid_portfolios = self.df[condition][self.specific_columns[0]].unique()
            self.df.loc[self.df[self.specific_columns[0]].isin(invalid_portfolios), 'calculation_type'] = '3X'
        except Exception as e:
            self.logger.error(f"Error in check_scr_delivery: {e}")

    def check_mandatory_columns(self):
        """
        Check if any mandatory column values are missing.
        """
        if not self.enable_checks.get("check_mandatory_columns", True):
            return
        try:
            for col in self.mandatory_columns:
                missing_rows = self.df[self.df[col].isnull()]
                if not missing_rows.empty:
                    for _, row in missing_rows.iterrows():
                        error_entry = {
                            "Error": f"Missing value in mandatory column: {col}",
                            "portfolio_id": row['portfolio_id'],
                            "id_of_underlying": row['id_of_underlying']
                        }
                        if col == "valuation_weight":
                            self.critical_errors.append(error_entry)
                            self.df.at[row.name, col] = 0  # Fill missing valuation_weight with 0
                        else:
                            self.validation_errors.append(error_entry)
                    self.df.loc[missing_rows.index, 'calculation_type'] = '3X'
        except Exception as e:
            self.logger.error(f"Error in check_mandatory_columns: {e}")

    def check_duplicate_rows(self):
        """
        Check for duplicate rows in the DataFrame.
        """
        if not self.enable_checks.get("check_duplicate_rows", True):
            return
        try:
            # Check for duplicates based on all mandatory columns
            duplicates = self.df[self.df.duplicated(subset=self.mandatory_columns, keep=False)]
            if not duplicates.empty:
                # Group by all mandatory columns to ensure all values are identical
                grouped_duplicates = duplicates.groupby(self.mandatory_columns).size().reset_index(name='count')
                exact_duplicates = grouped_duplicates[grouped_duplicates['count'] > 1]
                if not exact_duplicates.empty:
                    for _, row in exact_duplicates.iterrows():
                        affected_rows = self.df[(self.df[self.mandatory_columns] == row[self.mandatory_columns]).all(axis=1)]
                        for _, affected_row in affected_rows.iterrows():
                            self.validation_errors.append({
                                "Error": "Duplicate rows found",
                                "portfolio_id": affected_row['portfolio_id'],
                                "id_of_underlying": affected_row['id_of_underlying']
                            })
        except Exception as e:
            self.logger.error(f"Error in check_duplicate_rows: {e}")

    def check_data_types(self):
        """
        Check if the data types of the columns match the expected types.
        """
        if not self.enable_checks.get("check_data_types", True):
            return
        try:
            for col, expected_type in self.column_types.items():
                if not pd.api.types.is_dtype_equal(self.df[col].dtype, expected_type):
                    affected_rows = self.df[self.df[col].isnull()]
                    for _, row in affected_rows.iterrows():
                        self.validation_errors.append({
                            "Error": f"Column {col} is not of type {expected_type}",
                            "portfolio_id": row['portfolio_id'],
                            "id_of_underlying": row['id_of_underlying']
                        })
        except Exception as e:
            self.logger.error(f"Error in check_data_types: {e}")

    def check_portfolio_mod_dur(self):
        """
        Check if the sum of valuation_weight * instrument_mod_dur equals portfolio_mod_dur for each portfolio_id.
        """
        if not self.enable_checks.get("check_portfolio_mod_dur", True):
            return
        try:
            # Calculate weighted durations
            weighted_durations = self.df[self.specific_columns[6]] * self.df[self.specific_columns[8]]
            grouped = weighted_durations.groupby(self.df[self.specific_columns[0]]).sum()
            
            # Align indices before comparison
            portfolio_mod_dur = self.df.set_index(self.specific_columns[0])[self.specific_columns[2]].dropna()
            grouped, portfolio_mod_dur = grouped.align(portfolio_mod_dur, join='inner')
            
            # Identify mismatches
            mismatches = grouped[grouped != portfolio_mod_dur]
            if not mismatches.empty:
                affected_rows = self.df[self.df[self.specific_columns[0]].isin(mismatches.index)]
                for _, row in affected_rows.iterrows():
                    self.validation_errors.append({
                        "Error": "Sum of valuation_weight * instrument_mod_dur does not equal portfolio_mod_dur for some portfolio_id values",
                        "portfolio_id": row['portfolio_id'],
                        "id_of_underlying": row['id_of_underlying']
                    })
        except Exception as e:
            self.logger.error(f"Error in check_portfolio_mod_dur: {e}")

    def check_qc_of_underlying(self):
        """
        Check if the qc_of_underlying is not in the list of unique currencies and it is an IRR-related type,
        then change the calculation_type to "3X".
        """
        if not self.enable_checks.get("check_qc_of_underlying", True):
            return
        try:
            #Az elérhető pénznemek az eioparól
            unique_currencies = [
                "EUR", "BGN", "HRK", "CZK", "DKK", "HUF", "ISK", "CHF", "NOK", "PLN", 
                "RON", "SEK", "GBP", "AUD", "CAD", "CLP", "JPY", "KRW", "MXN", 
                "NZD", "TRY", "USD"
            ]
            
            condition = (~self.df['qc_of_underlying'].isin(unique_currencies)) & (self.df['underlying_asset_category'].isin(self.irr_related_types))
            self.df.loc[condition, 'calculation_type'] = '3X'
        except Exception as e:
            self.logger.error(f"Error in check_qc_of_underlying: {e}")

class LevelOneValidator(BaseValidator, ABC):
    def __init__(self, level_one_data_df, tpt_data_df, oecd_countries, specific_columns=None, column_types=None, mandatory_columns=None, enable_checks=None, irr_related_types=None):
        """
        Létrehozzuk az alap elemet a LevelOneValidator class-ban.
        
        Args:
        - level_one_data_df (pd.DataFrame): A DataFrame, amely az új adatokat tartalmazza.
        - tpt_data_df (pd.DataFrame): A már validált TPT adatokat tartalmazó DataFrame.
        - oecd_countries (list): Az OECD országok listája.
        - specific_columns (list): Az egyes validációkhoz használt specifikus oszlopok listája.
        - column_types (dict): Szótár, amely az oszlopneveket a várt adattípusokhoz rendeli.
        - mandatory_columns (list): A kötelező oszlopok listája, amelyeket ellenőrizni kell a hiányzó értékek szempontjából.
        - enable_checks (dict): Szótár, amely lehetővé teszi vagy letiltja az egyes ellenőrzéseket.
        """
        super().__init__(level_one_data_df)
        
        self.level_one_data_df = level_one_data_df
        self.tpt_data_df = tpt_data_df
        self.oecd_countries = oecd_countries

        self.column_types = column_types or {
            "portfolio_group": str,
            "portfolio_id": str,
            "analysis_date": 'datetime64[ns]',
            "security_name": str,
            "country": str,
            "rating": str,
            "cic_of_instrument": str,
            "asset_allocation": str,
            "currency": str,
            "exp_maturity_date": 'datetime64[ns]',
            "balance_nominal": float,
            "modified_duration": float,
            "dirty_value_pc": float,
        }

        self.mandatory_columns = mandatory_columns or [
            "portfolio_group", "portfolio_id", "dirty_value_pc"
        ]

        self.irr_related_types = irr_related_types or [
            '1', '2', '4', '5', '6', '8'
        ]

        self.specific_columns = specific_columns or list(self.column_types.keys())

        self.enable_checks = enable_checks or {
            "check_mandatory_columns": True,
            "check_data_types": True,
            "check_calculation_type": True,
            "check_missing_ratings": True,
            "check_total_vs_value_col": True,
            "check_irr_related_types": True
        }

        # Az oszlopok átalakítása a várt adattípusokra és a kötelező oszlopok átnézése
        self.convert_column_types(self.level_one_data_df, self.column_types)

        # A dirty_value_pc oszlop összegzése
        self.total_dirty_value_pc = self.level_one_data_df['dirty_value_pc'].sum()
        
        # Azoknak a soroknak az eltávolítása, ahol hiányzik a portfolio_id
        self.level_one_data_df.dropna(subset=['portfolio_id'], inplace=True)

        # Left join az level_one_data_df-re és megnézni melyek azok a sorok, ahol nincs
        self.level_one_calc_df = self.level_one_data_df.merge(self.tpt_data_df, how='left', left_on='portfolio_id', right_on='portfolio_id')
        self.unmatched_indexes = self.level_one_calc_df[self.level_one_calc_df['calculation_type'].isnull()].index

        #Hiányzó currency-k és dátumok kitöltése
        self.fill_qc_of_underlying()
        self.fill_maturity_date()
        self.fill_valuation_date()
        self.fill_cqs()
        self.fill_modified_duration()

        # Validációs hibák és kritikus hibák inicializálása
        self.validation_errors = []
        self.critical_errors = []

    def convert_column_types(self, df, column_types):
        """
        Az oszlopok átalakítása a várt adattípusokra.
        """
        for col, col_type in column_types.items():
            try:
                if col == 'exp_maturity_date':
                    df[col] = df[col].apply(self.convert_maturity_date)
                if col_type == 'datetime64[ns]':
                    # Explicit módon átalakítjuk datetime64[ns] típusra
                    df[col] = pd.to_datetime(df[col], format='%Y.%m.%d', errors='coerce')
                elif col_type == 'timedelta64[ns]':
                    # Explicit módon átalakítjuk timedelta64[ns] típusra
                    df[col] = pd.to_timedelta(df[col], errors='coerce')
                else:
                    df[col] = df[col].astype(col_type)
            except ValueError as e:
                affected_rows = df[df[col].astype(str).str.contains(',', na=False)]
                for _, row in affected_rows.iterrows():
                    self.validation_errors.append({
                        "Error": f"Az {col} oszlopot nem sikerült átalakítani {col_type} típusra: {e}",
                        "portfolio_id": row['portfolio_id'],
                        "id_of_underlying": row['id_of_underlying']
                    })
                self.logger.error(f"Az {col} oszlopot nem sikerült átalakítani {col_type} típusra: {e}")

    def fill_modified_duration(self):
        """
        Kitölti a mod_dur_of_underlying oszlopot a modified_duration oszloppal.
        """
        try:
            unmatched_rows = self.level_one_calc_df.loc[self.unmatched_indexes].copy()
            self.level_one_calc_df.loc[self.unmatched_indexes, 'mod_dur_of_underlying'] = unmatched_rows['modified_duration']
        except Exception as e:
            self.logger.error(f"Hiba a fill_modified_durationben: {e}")

    def fill_qc_of_underlying(self):
        """
        Kitölti a qc_of_underlying oszlopot a currency oszlop értékeivel az unmatched soroknál.
        """
        try:
            unmatched_rows = self.level_one_calc_df.loc[self.unmatched_indexes]
            self.level_one_calc_df.loc[self.unmatched_indexes, 'qc_of_underlying'] = unmatched_rows['currency']
        except Exception as e:
            self.logger.error(f"Hiba a fill_qc_of_underlying metódusban: {e}")

    def fill_maturity_date(self):
        """
        Kitölti a maturity_date oszlopot az exp_maturity_date oszlop értékeivel az unmatched soroknál.
        """
        try:
            unmatched_rows = self.level_one_calc_df.loc[self.unmatched_indexes].copy()
            self.level_one_calc_df.loc[self.unmatched_indexes, 'maturity_date'] = unmatched_rows['exp_maturity_date']
        except Exception as e:
            self.logger.error(f"Hiba a fill_maturity_date metódusban: {e}")

    def fill_valuation_date(self):
        """
        Kitölti a valuation_date oszlopot az analysis_date oszlop értékeivel az unmatched soroknál.
        """
        try:
            unmatched_rows = self.level_one_calc_df.loc[self.unmatched_indexes].copy()
            self.level_one_calc_df.loc[self.unmatched_indexes, 'valuation_date'] = unmatched_rows['analysis_date']
        except Exception as e:
            self.logger.error(f"Hiba a fill_valuation_date metódusban: {e}")

    def fill_cqs(self):
        """
        Kitölti a cqs oszlopot a rating oszlop értékei alapján az unmatched soroknál.
        """
        try:
            # Filter the unmatched rows
            unmatched_rows = self.level_one_calc_df.loc[self.unmatched_indexes].copy()

            # Strip the rating from spaces, + and -
            unmatched_rows['stripped_rating'] = unmatched_rows['rating'].str.replace(r'[+\-\s]', '', regex=True)

            # Map the stripped ratings to numeric values
            rating_mapping = {
                'AAA': 0,
                'AA': 1,
                'A': 2,
                'BBB': 3,
                'BB': 4,
                'B': 5,
                'NR': 'NR'
            }
            unmatched_rows['cqs'] = unmatched_rows['stripped_rating'].map(rating_mapping)

            # Set cqs to 6 if stripped_rating is not NaN, not 'NR', and not in rating_mapping
            unmatched_rows.loc[unmatched_rows['stripped_rating'].notna() & unmatched_rows['cqs'].isna(), 'cqs'] = 6

            # Convert cqs to integer where applicable
            unmatched_rows['cqs'] = unmatched_rows['cqs'].astype(object)

            # Update the original DataFrame with the modified values
            self.level_one_calc_df.loc[self.unmatched_indexes, 'cqs'] = unmatched_rows['cqs']

        except Exception as e:
            self.logger.error(f"Hiba a fill_cqs metódusban: {e}")

    def check_mandatory_columns(self):
        """
        Ellenőrizzük, hogy a kötelező oszlopokban vannak-e hiányzó értékek.
        """
        if not self.enable_checks.get("check_mandatory_columns", True):
            return
        try:
            for col in self.mandatory_columns:
                missing_rows = self.level_one_data_df[self.level_one_data_df[col].isnull()]
                if not missing_rows.empty:
                    for _, row in missing_rows.iterrows():
                        error_entry = {
                            "Error": f"Hiányzó érték a kötelező oszlopban: {col}",
                            "portfolio_id": row['portfolio_id']
                        }
                        if col == "dirty_value_pc":
                            self.critical_errors.append(error_entry)
                            self.level_one_data_df.at[row.name, col] = 0
                        else:
                            self.validation_errors.append(error_entry)
                    self.level_one_data_df.loc[missing_rows.index, 'calculation_type'] = '3X'
        except Exception as e:
            self.logger.error(f"Hiba a check_mandatory_columns metódusban: {e}")

    @abstractmethod
    def check_calculation_type(self):
        """
        Abstract method to be implemented by subclasses to check and fill missing calculation_type values.
        """
        pass

    def check_missing_ratings(self):
        """
        Ellenőrizzük, hogy a calculation_type 1, 2, 8 esetén hiányzik-e a rating,
        csak azoknál a soroknál, ahol nincs egyező portfolio_id az level_one_data_df-ben.
        """
        try:
            unmatched_rows = self.level_one_calc_df.loc[self.unmatched_indexes].copy()

            # Feltétel ellenőrzése: calculation_type 1, 2, 8 és hiányzó rating
            condition = unmatched_rows['calculation_type'].isin(['1', '2', '8']) & unmatched_rows['rating'].isnull()

            if condition.any():
                # Update calculation_type for rows that meet the condition
                self.level_one_calc_df.loc[self.unmatched_indexes[condition], 'calculation_type'] = '3X'
                for idx in self.unmatched_indexes[condition]:
                    row = self.level_one_calc_df.loc[idx]
                    self.validation_errors.append({
                        "Error": "Hiányzó rating értékek a calculation_type 1, 2, 8 esetén",
                        "portfolio_id": row['portfolio_id']
                    })
        except Exception as e:
            self.logger.error(f"Hiba a check_missing_ratings metódusban: {e}")

    def check_total_vs_value_col(self):
        """
        Érték oszlop hozzáadása és az összérték ellenőrzése.
        """
        try:
            # Hiányzó valuation_weight értékek kitöltése 1-gyel
            self.level_one_calc_df['valuation_weight'].fillna(1, inplace=True)

            # Érték oszlop hozzáadása
            self.level_one_calc_df['value'] = self.level_one_calc_df['valuation_weight'] * self.level_one_calc_df['dirty_value_pc']

            # Az érték oszlop összegzése
            total_value = self.level_one_calc_df['value'].sum()

            # Ellenőrizzük, hogy az összérték +/- 10000-en belül van-e a total_dirty_value_pc-hez képest
            if not (self.total_dirty_value_pc - 1000000 <= total_value <= self.total_dirty_value_pc + 1000000):
                error_entry = {
                    "Error": "Az összérték nincs +/- 1000000-en belül a total_dirty_value_pc-hez képest",
                    "total_dirty_value_pc": self.total_dirty_value_pc,
                    "total_value": total_value
                }
                self.critical_errors.append(error_entry)
                self.logger.error("Az összérték nincs +/- 1000000-en belül a total_dirty_value_pc-hez képest")
        except Exception as e:
            self.logger.error(f"Hiba az add_value_column_and_check_total metódusban: {e}")

    def check_data_types(self):
        """
        Ellenőrizzük, hogy az oszlopok adattípusai megfelelnek-e a várt típusoknak.
        """
        if not self.enable_checks.get("check_data_types", True):
            return
        try:
            for col, expected_type in self.column_types.items():
                if not pd.api.types.is_dtype_equal(self.level_one_calc_df[col].dtype, expected_type):
                    affected_rows = self.level_one_calc_df[self.level_one_calc_df[col].isnull()]
                    for _, row in affected_rows.iterrows():
                        self.validation_errors.append({
                            "Error": f"Az {col} oszlop nem a várt {expected_type} típusú",
                            "portfolio_id": row['portfolio_id']
                        })
        except Exception as e:
            self.logger.error(f"Hiba a check_data_types metódusban: {e}")

    def check_irr_related_types(self):
        """
        Ellenőrizzük, hogy az IRR-hez kapcsolódó típusok esetén hiányzik-e a modified_duration vagy a maturity_date,
        csak azoknál a soroknál, ahol nincs egyező portfolio_id az level_one_data_df-ben.
        """
        if not self.enable_checks.get("check_irr_related_types", True):
            return
        try:
            # Csak azok a sorok, ahol nincs egyező portfolio_id az level_one_data_df-ben
            unmatched_rows = self.level_one_calc_df.loc[self.unmatched_indexes].copy()

            # Feltétel ellenőrzése: IRR-hez kapcsolódó calculation_type és hiányzó modified_duration vagy maturity_date
            condition = unmatched_rows['calculation_type'].isin(self.irr_related_types) & (
                (unmatched_rows['modified_duration'].isnull()) | (unmatched_rows['maturity_date'].isnull()) | (unmatched_rows['analysis_date'].isnull())
            )
            
            if condition.any():
                self.level_one_calc_df.loc[self.unmatched_indexes[condition], 'calculation_type'] = '3X'
                for idx in unmatched_rows[condition].index:
                    row = self.level_one_calc_df.loc[idx]
                    self.validation_errors.append({
                        "Error": "Hiányzó modified_duration vagy maturity_date értékek az IRR-hez kapcsolódó calculation_type esetén",
                        "portfolio_id": row['portfolio_id']
                    })
        except Exception as e:
            self.logger.error(f"Hiba a check_irr_related_types metódusban: {e}")

    def validate(self):
        """
        Az összes validációs ellenőrzés futtatása.
        """
        self.check_mandatory_columns()
        self.check_data_types()
        self.check_calculation_type()
        self.check_missing_ratings()
        self.check_total_vs_value_col()
        self.check_irr_related_types()

        validation_report = pd.DataFrame([
            {
                "Error": error["Error"],
                "portfolio_id": error["portfolio_id"],
            }
            for error in self.validation_errors
        ])

        critical_report = pd.DataFrame([
            {
                "Error": error["Error"],
                "portfolio_id": error.get("portfolio_id"),
                "total_dirty_value_pc": error.get("total_dirty_value_pc"),
                "total_value": error.get("total_value")
            }
            for error in self.critical_errors
        ])

        return self.level_one_calc_df, validation_report, critical_report
    
class AssetListValidator(LevelOneValidator):
    def __init__(self, level_one_data_df, tpt_data_df, oecd_countries, specific_columns=None, column_types=None, mandatory_columns=None, enable_checks=None, irr_related_types=None):
        """
        Inicializálja az AssetListValidator osztályt.
        
        Args:
        - level_one_data_df (pd.DataFrame): A DataFrame, amely az új adatokat tartalmazza.
        - tpt_data_df (pd.DataFrame): A már validált TPT adatokat tartalmazó DataFrame.
        - oecd_countries (list): Az OECD országok listája.
        - specific_columns (list): Az egyes validációkhoz használt specifikus oszlopok listája.
        - column_types (dict): Szótár, amely az oszlopneveket a várt adattípusokhoz rendeli.
        - mandatory_columns (list): A kötelező oszlopok listája, amelyeket ellenőrizni kell a hiányzó értékek szempontjából.
        - enable_checks (dict): Szótár, amely lehetővé teszi vagy letiltja az egyes ellenőrzéseket.
        - irr_related_types (list): Az IRR-hez kapcsolódó típusok listája.
        """
        super().__init__(level_one_data_df, tpt_data_df, oecd_countries, specific_columns, column_types, mandatory_columns, enable_checks, irr_related_types)

    def check_calculation_type(self):
        """
        Hiányzó calculation_type értékek kitöltése a cic_code és egyéb kritériumok alapján.
        """
        try:
            unmatched_rows = self.level_one_calc_df.loc[self.unmatched_indexes].copy()
            for idx, row in unmatched_rows.iterrows():
                cic_code = row['cic_of_instrument']
                asset_allocation = row['asset_allocation']
                security_name = row['security_name']

                if pd.isnull(cic_code) or cic_code == '0' or len(cic_code) != 4:
                    calc_type = '3X'
                elif asset_allocation.startswith('645'):
                    calc_type = 'FX'
                elif asset_allocation.startswith('996'): #A depositra úgy kell számolni, mint a bondokra, not cash equivalent
                    calc_type = '2'
                elif str(cic_code[2]) == '3':
                    if asset_allocation.startswith('950'):
                        calc_type = '3S'
                    elif str(cic_code[:2]) in self.oecd_countries:
                        calc_type = '3L'
                    else:
                        calc_type = '3X'
                else:
                    calc_type = str(cic_code[2])

                if calc_type == '1':
                    contains_keywords = pd.Series([security_name]).str.contains(r'(?:EIB|HGB|HTB)', case=False).iloc[0]
                    starts_with_hu = cic_code.startswith('HU')
                    
                    if not contains_keywords and not starts_with_hu:
                        calc_type = '1X'

                self.level_one_calc_df.at[idx, 'calculation_type'] = calc_type
        except Exception as e:
            self.logger.error(f"Hiba a check_calculation_type metódusban: {e}")

class UnitLinkedListValidator(LevelOneValidator):
    def __init__(self, level_one_data_df, tpt_data_df, oecd_countries, specific_columns=None, column_types=None, mandatory_columns=None, enable_checks=None, irr_related_types=None):
        """
        Inicializálja a UnitLinkedListValidator osztályt.
        
        Args:
        - level_one_data_df (pd.DataFrame): A DataFrame, amely az új adatokat tartalmazza.
        - tpt_data_df (pd.DataFrame): A már validált TPT adatokat tartalmazó DataFrame.
        - oecd_countries (list): Az OECD országok listája.
        - specific_columns (list): Az egyes validációkhoz használt specifikus oszlopok listája.
        - column_types (dict): Szótár, amely az oszlopneveket a várt adattípusokhoz rendeli.
        - mandatory_columns (list): A kötelező oszlopok listája, amelyeket ellenőrizni kell a hiányzó értékek szempontjából.
        - enable_checks (dict): Szótár, amely lehetővé teszi vagy letiltja az egyes ellenőrzéseket.
        - irr_related_types (list): Az IRR-hez kapcsolódó típusok listája.
        """
        super().__init__(level_one_data_df, tpt_data_df, oecd_countries, specific_columns, column_types, mandatory_columns, enable_checks, irr_related_types)

    def check_calculation_type(self):
        """
        Hiányzó calculation_type értékek kitöltése a cic_code és egyéb kritériumok alapján.
        """
        try:
            unmatched_rows = self.level_one_calc_df.loc[self.unmatched_indexes].copy()
            for idx, row in unmatched_rows.iterrows():
                cic_code = row['cic_of_instrument']
                security_name = row['security_name']

                if pd.isnull(cic_code) or cic_code == '0' or len(cic_code) != 4:
                    calc_type = '3X'
                elif str(cic_code[2]) == '3':
                    if str(cic_code[:2]) in self.oecd_countries:
                        calc_type = '3L'
                    else:
                        calc_type = '3X'
                else:
                    calc_type = str(cic_code[2])

                if calc_type == '1':
                    contains_keywords = pd.Series([security_name]).str.contains(r'(?:EIB|HGB|HTB)', case=False).iloc[0]
                    starts_with_hu = cic_code.startswith('HU')
                    
                    if not contains_keywords and not starts_with_hu:
                        calc_type = '1X'

                self.level_one_calc_df.at[idx, 'calculation_type'] = calc_type
        except Exception as e:
            self.logger.error(f"Hiba a check_calculation_type metódusban: {e}")