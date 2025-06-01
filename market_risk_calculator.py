#Importok
import pandas as pd
import json
from base_functions import dataframe_from_xlsx, write_dataframes_to_excel
from file_fetching import run_all_fetching
from data_validation import TPTDataValidator, AssetListValidator, UnitLinkedListValidator
from risk_calculation import *
import sys

input_file = '../template/market_risk_template.xlsx'

def calculate_everything(input_file_path):
    '''
    Ez a függvény összekapcsol eddigi minden kódot
    Letölti a szükséges fileokat
    Adatokat validál
    Risket számol
    '''
    #File élérési útvonalak meghatározása
    input_df = dataframe_from_xlsx(input_file_path, 'input_df', header = True)
    done_file_path   = input_df.loc[input_df['file_name'] == 'done_file', 'file_path'].values[0]
    tpt_part_1       = input_df.loc[input_df['file_name'] == 'tpt_data_part1', 'file_path'].values[0]
    tpt_part_2       = input_df.loc[input_df['file_name'] == 'tpt_data_part2', 'file_path'].values[0]
    tpt_part_3       = input_df.loc[input_df['file_name'] == 'tpt_data_part3', 'file_path'].values[0]
    asset_list       = input_df.loc[input_df['file_name'] == 'asset_list', 'file_path'].values[0]
    ul_list          = input_df.loc[input_df['file_name'] == 'ul_list', 'file_path'].values[0]
    tpt_validation   = input_df.loc[input_df['file_name'] == 'tpt_validation', 'file_path'].values[0]
    asset_validation = input_df.loc[input_df['file_name'] == 'asset_validation', 'file_path'].values[0]
    ul_validation    = input_df.loc[input_df['file_name'] == 'ul_validation', 'file_path'].values[0]
    parameters       = input_df.loc[input_df['file_name'] == 'parameters', 'file_path'].values[0]
    sym_adj_path     = input_df.loc[input_df['file_name'] == 'sym_adj', 'file_path'].values[0]
    rfr_zip_path     = input_df.loc[input_df['file_name'] == 'rfr_rates', 'file_path'].values[0]
    fx_rates_path    = input_df.loc[input_df['file_name'] == 'exchange_rates', 'file_path'].values[0]

    rfr_file_path    = run_all_fetching(input_file_path)

    calculation_matrix = dataframe_from_xlsx(input_file_path, 'calculation_matrix', header = True)

    #Szükséges extra paraméterek beolvasása
    with open(parameters, 'r') as file:
        data = json.load(file)
    oecd_eea_countries = data['EEA_COUNTRIES'] + data['OECD_NON_EEA_COUNTRIES']
    calculation_type_mapping = data['calculation_type_mapping']

    #TPT, Asset list és UL List dataframek előkészítése
    tpt_df_1 = pd.read_csv(tpt_part_1, encoding='ISO-8859-1', low_memory=False, sep = ';')
    tpt_df_2 = pd.read_csv(tpt_part_2, encoding='ISO-8859-1', low_memory=False, sep = ';')
    tpt_df_3 = pd.read_csv(tpt_part_3, encoding='ISO-8859-1', low_memory=False, sep = ';')

    tpt_df_1.dropna(subset = ['1_Portfolio_identifying_data'], inplace=True)
    tpt_df_2.dropna(subset = ['1_Portfolio_identifying_data'], inplace=True)
    tpt_df_3.dropna(subset = ['1_Portfolio_identifying_data'], inplace=True)

    tpt_combined = pd.concat([tpt_df_1, tpt_df_2, tpt_df_3], ignore_index=True)

    #TPT dataframe oszlopai
    tpt_selected_columns = ['1_Portfolio_identifying_data', 
                        '6_Valuation_date',
                        '10_Portfolio_modified_duration', 
                        '11_Complete_SCR_delivery',
                        '12_CIC_code_of_the_instrument',
                        '13_Economic_zone_of_the_quotation_place',
                        '14_Identification_code_of_the_instrument',
                        '17_Instrument_name',
                        '21_Quotation_currency_(A)',
                        '26_Valuation_weight', 
                        '39_Maturity_date', 
                        '59_Credit_quality_step',
                        '90_Modified_duration_to_maturity_date',
                        '131_Underlying_asset_category']

    #TPT dataframe újranevezése
    tpt_new_column_names = {"1_Portfolio_identifying_data": "portfolio_id",
                        "6_Valuation_date": "valuation_date", 
                        "10_Portfolio_modified_duration": "portfolio_mod_dur", 
                        "11_Complete_SCR_delivery": "scr_delivery", 
                        "12_CIC_code_of_the_instrument": "cic_of_underlying", 
                        "13_Economic_zone_of_the_quotation_place": "economic_zone", 
                        "14_Identification_code_of_the_instrument": "id_of_underlying", 
                        "17_Instrument_name": "name_of_underlying",
                        "21_Quotation_currency_(A)": "qc_of_underlying", 
                        "26_Valuation_weight": "valuation_weight", 
                        "39_Maturity_date": "maturity_date", 
                        "59_Credit_quality_step": "cqs", 
                        "90_Modified_duration_to_maturity_date": "mod_dur_of_underlying", 
                        "131_Underlying_asset_category": "underlying_asset_category"}

    #Csak a kiválasztott oszlopok megtartása és újranevezése
    tpt_selected = tpt_combined.loc[:, tpt_selected_columns].rename(columns=tpt_new_column_names)



    asset_list_df = pd.read_excel(asset_list)
    asset_selected_columns = [
        'Analysis date',
        'Security ID', 
        'Security name', 
        'Coun- try',
        'Solvency2 WP Rating', 
        'CIC Code', 
        'Asset Allocation 5. Ebene', 
        'Currency', 
        'Expected maturity date', 
        'Balance nominal/number', 
        'Modified duration YTM', 
        'Dirty value PC', 
        'Portfolio'
    ]
    asset_new_column_names = {
        'Analysis date': 'analysis_date',
        'Security ID': 'portfolio_id',
        'Security name': 'security_name',
        'Coun- try': 'country',
        'Solvency2 WP Rating': 'rating',
        'CIC Code': 'cic_of_instrument',
        'Asset Allocation 5. Ebene': 'asset_allocation',
        'Currency': 'currency',
        'Expected maturity date': 'exp_maturity_date',
        'Balance nominal/number': 'balance_nominal',
        'Modified duration YTM': 'modified_duration',
        'Dirty value PC': 'dirty_value_pc',
        'Portfolio': 'portfolio_group'
    }

    #Asset list df létrehozása
    asset_list_df = asset_list_df[asset_selected_columns].rename(columns=asset_new_column_names)

    #Oszlopok újrarendezése
    asset_ordered_columns = [
        'portfolio_group', 'portfolio_id', 'analysis_date', 'security_name', 
        'country', 'rating', 'cic_of_instrument', 'asset_allocation', 
        'currency', 'exp_maturity_date', 'balance_nominal', 'modified_duration', 
        'dirty_value_pc'
    ]
    asset_list_df = asset_list_df[asset_ordered_columns]



    ul_list_df = pd.read_excel(ul_list)

    ul_selected_columns = [
        'Analysis date',
        'Security ID', 
        'Security name', 
        'Coun- try',
        'Solvency2 WP Rating', 
        'CIC Code', 
        'Asset Allocation 5. Ebene', 
        'Quotation currency', 
        'Maturity date', 
        'Balance nominal/number', 
        'Modified duration YTM', 
        'Dirty value RC', 
        'Portfolio group'
    ]
    ul_new_column_names = {
        'Analysis date': 'analysis_date',
        'Security ID': 'portfolio_id',
        'Security name': 'security_name',
        'Coun- try': 'country',
        'Solvency2 WP Rating': 'rating',
        'CIC Code': 'cic_of_instrument',
        'Asset Allocation 5. Ebene': 'asset_allocation',
        'Quotation currency': 'currency',
        'Maturity date': 'exp_maturity_date',
        'Balance nominal/number': 'balance_nominal',
        'Modified duration YTM': 'modified_duration',
        'Dirty value RC': 'dirty_value_pc',
        'Portfolio group': 'portfolio_group'
    }

    #UL df szűrése és átnevezése
    ul_list_df = ul_list_df[ul_selected_columns].rename(columns=ul_new_column_names)

    #UL df újrarendezése
    ul_ordered_columns = [
        'portfolio_group', 'portfolio_id', 'analysis_date', 'security_name', 
        'country', 'rating', 'cic_of_instrument', 'asset_allocation', 
        'currency', 'exp_maturity_date', 'balance_nominal', 'modified_duration', 
        'dirty_value_pc'
    ]
    ul_list_df = ul_list_df[ul_ordered_columns]



    #Adatok validálása
    tpt_validator = TPTDataValidator(tpt_selected)
    tpt_calc_df, tpt_validation_report, tpt_critical_report = tpt_validator.validate()
    tpt_validator.save_validation_report(tpt_validation_report, tpt_critical_report, tpt_validation)

    asset_validator = AssetListValidator(asset_list_df, tpt_calc_df, oecd_eea_countries)
    asset_calc_df, asset_validation_report, asset_critical_report = asset_validator.validate()
    asset_validator.save_validation_report(asset_validation_report, asset_critical_report, asset_validation)

    ul_validator = UnitLinkedListValidator(ul_list_df, tpt_calc_df, oecd_eea_countries)
    ul_calc_df, ul_validation_report, ul_critical_report = ul_validator.validate()
    ul_validator.save_validation_report(ul_validation_report, ul_critical_report, ul_validation)


    #Kockázat számolása
    al_eq_df, al_irr_df, al_spr_df, al_curr_df, al_conc_df, al_prop_df = calculate_asset_list_market_risk(asset_calc_df, calculation_matrix, calculation_type_mapping, parameters, sym_adj_path, rfr_file_path, fx_rates_path)
    ul_eq_df, ul_irr_df, ul_spr_df, ul_curr_df, ul_prop_df = calculate_ul_list_market_risk(ul_calc_df, calculation_matrix, calculation_type_mapping, parameters, sym_adj_path, rfr_file_path, fx_rates_path)

    #Output előkészítése
    asset_list_dataframes = [al_eq_df, al_irr_df, al_spr_df, al_curr_df, al_conc_df, al_prop_df]
    ul_list_dataframes = [ul_eq_df, ul_irr_df, ul_spr_df, ul_curr_df, ul_prop_df]
    validated_dataframes = [asset_calc_df, ul_calc_df]


    #Kimentési folyamat
    asset_list_sheet_names = ['equity_check', 'interest_rate_check', 'spread_check', 'currency_check', 'conc_check', 'property_check']
    ul_list_sheet_names = ['equity_check', 'interest_rate_check', 'spread_check', 'currency_check', 'property_check']

    asset_list_starting_cells = ['I4', 'I4', 'I4', 'I4', 'I4', 'I4']
    ul_list_starting_cells = ['R4', 'U4', 'R4', 'S4', 'P4']

    dataframes = asset_list_dataframes + ul_list_dataframes
    sheet_names = asset_list_sheet_names + ul_list_sheet_names
    starting_cells = asset_list_starting_cells + ul_list_starting_cells

    write_dataframes_to_excel(input_file_path, done_file_path, dataframes, sheet_names, starting_cells)

    return asset_list_dataframes, ul_list_dataframes, validated_dataframes

def calculate_asset_list_market_risk(asset_list_df, calculation_matrix, calculation_type_mapping, param_file, sym_adj_file, rfr_rates_file, exchange_rates_file):
    '''
    Kiszámítja az asset listre a market risket
    '''
    asset_list_equity_df = calculate_equity_risk(asset_list_df, sym_adj_file, param_file, calculation_matrix, calculation_type_mapping)
    asset_list_irr_df = calculate_interest_rate_risk(asset_list_df, param_file, rfr_rates_file, calculation_matrix, calculation_type_mapping)
    asset_list_spread_df = calculate_spread_risk(asset_list_df, param_file, calculation_matrix, calculation_type_mapping)
    asset_list_currency_df =  calculate_currency_risk(asset_list_df, param_file, exchange_rates_file, calculation_matrix, calculation_type_mapping)
    asset_list_concentration_df = calculate_concentration_risk(asset_list_df, param_file, calculation_matrix, calculation_type_mapping)
    asset_list_property_df = calculate_property_risk(asset_list_df, param_file, calculation_matrix, calculation_type_mapping)

    return asset_list_equity_df, asset_list_irr_df, asset_list_spread_df, asset_list_currency_df, asset_list_concentration_df, asset_list_property_df

def calculate_ul_list_market_risk(ul_list_df, calculation_matrix, calculation_type_mapping, param_file, sym_adj_file, rfr_rates_file, exchange_rates_file):
    '''
    Kiszámítja a ul listre a market risket
    '''
    ul_list_equity_df = calculate_equity_risk(ul_list_df, sym_adj_file, param_file, calculation_matrix, calculation_type_mapping)
    ul_list_irr_df = calculate_interest_rate_risk(ul_list_df, param_file, rfr_rates_file, calculation_matrix, calculation_type_mapping)
    ul_list_spread_df = calculate_spread_risk(ul_list_df, param_file, calculation_matrix, calculation_type_mapping)
    ul_list_currency_df =  calculate_currency_risk(ul_list_df, param_file, exchange_rates_file, calculation_matrix, calculation_type_mapping)
    ul_list_property_df = calculate_property_risk(ul_list_df, param_file, calculation_matrix, calculation_type_mapping)

    return ul_list_equity_df, ul_list_irr_df, ul_list_spread_df, ul_list_currency_df, ul_list_property_df

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python market_risk_calculator.py <input_file_path>")
        sys.exit(1)
    
    input_file_path = sys.argv[1]
    calculate_everything(input_file_path)