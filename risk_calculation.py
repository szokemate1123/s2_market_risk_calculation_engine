#Importok
import pandas as pd
import json
from file_fetcher_class import extract_symmetric_adjustment
from datetime import datetime
import numpy as np

#Paraméterek beolvasása


#Kódok a risk kalkulációhoz
def calculate_equity_risk(data, sym_adj_file, param_file, calculation_matrix, calculation_type_mapping):
    #Paraméterek beolvasása a JSON fileból
    with open(param_file, 'r') as file:
        parameters = json.load(file)
    equity_risk_params = parameters['equity_risk']

    #Szűrjük a dataframe-t azokra a type-okra, amire számolunk risket
    equity_risk_types = [key for key, value in calculation_type_mapping.items() if calculation_matrix.iloc[value]['Equity Risk'] == 'x'] + ['3S']
    filtered_data = data[data['calculation_type'].isin(equity_risk_types)].copy()

    #Korábban mentett sym_adj érték kiolvasása
    symmetric_adjustment = extract_symmetric_adjustment(sym_adj_file)

    #Shockok beállítása
    strategic_shock = equity_risk_params['strategic_shock']
    t1_shock = equity_risk_params['T1_base_shock'] + symmetric_adjustment
    t2_shock = equity_risk_params['T2_base_shock'] + symmetric_adjustment

    #SF szerinti shockok alkalmazására függvény
    filtered_data['shocked_value'] = np.where(
        filtered_data['calculation_type'] == '3S',
        filtered_data['value'] * (1 - strategic_shock),
        np.where(
            filtered_data['calculation_type'] == '3L',
            filtered_data['value'] * (1 - t1_shock),
            filtered_data['value'] * (1 - t2_shock)
        )
    )

    filtered_data['equity_type'] = np.where(
        filtered_data['calculation_type'] == '3S',
        'strat_part',
        np.where(
            filtered_data['calculation_type'] == '3L',
            't1_equity',
            't2_equity'
        )
    )

    filtered_data['calc_equity_shock'] = filtered_data['value'] - filtered_data['shocked_value']

    #Dataframe kimentéséhez átalakítjuk kicsit
    output_equity = filtered_data.groupby(['portfolio_id', 'portfolio_group']).agg({
    'security_name': 'first',
    'value': 'sum',
    'shocked_value': 'sum',
    'calc_equity_shock': 'sum',
    'cic_of_instrument': 'first',
    'equity_type': lambda x: filtered_data.loc[x.index].groupby('equity_type')['value'].sum().idxmax()
    }).reset_index()

    #Oszlopok sorbarendezése
    output_equity = output_equity[[
        'portfolio_group', 
        'portfolio_id', 
        'security_name', 
        'equity_type', 
        'cic_of_instrument', 
        'value', 
        'shocked_value', 
        'calc_equity_shock'
    ]]

    return output_equity

def calculate_interest_rate_risk(data, param_file, rfr_rates_file, calculation_matrix, calculation_type_mapping):
    # Read the parameters from the JSON file
    with open(param_file, 'r') as file:
        parameters = json.load(file)
    country_currency_mapping = parameters['COUNTRY_CURRENCY_MAPPING']

    # Filter the DataFrame for interest rate risk relevant types
    interest_rate_risk_types = [key for key, value in calculation_type_mapping.items() if calculation_matrix.iloc[value]['Interest rate risk'] == 'x'] + ['1X']
    filtered_data = data[data['calculation_type'].isin(interest_rate_risk_types)].copy()

    # Ensure the date columns are in datetime format
    filtered_data['maturity_date'] = pd.to_datetime(filtered_data['maturity_date'], format='%Y.%m.%d', errors='coerce')
    filtered_data['analysis_date'] = pd.to_datetime(filtered_data['analysis_date'], format='%Y.%m.%d', errors='coerce')

    # Check for missing dates and throw an error if any are missing
    missing_dates = filtered_data[filtered_data['maturity_date'].isnull() | filtered_data['analysis_date'].isnull()]
    if not missing_dates.empty:
        raise ValueError(f"Missing maturity_date or analysis_date in the data for rows: {missing_dates.index.tolist()}")

    # Apply the function to calculate years_to_maturity
    filtered_data['years_to_maturity'] = filtered_data.apply(calculate_years_to_maturity, axis=1)

    # Read the spot values DataFrame
    rfr_no_va        = pd.read_excel(rfr_rates_file, sheet_name = 'RFR_spot_no_VA')
    up_shock_rates   = pd.read_excel(rfr_rates_file, sheet_name = 'Spot_NO_VA_shock_UP')
    down_shock_rates = pd.read_excel(rfr_rates_file, sheet_name = 'Spot_NO_VA_shock_DOWN')

    def interpolate_rfr(row, rfr_values):
        '''
        Lineáris interpoláció az RFR értékeknél a megfelelő years_to_maturity-hez
        '''
        currency = row['qc_of_underlying']
        years = row['years_to_maturity']

        if currency == 'EUR':
            rfr_row = rfr_values[rfr_values['Unnamed: 1'].str.startswith('EUR')]
        else:
            country_code = next(key for key, value in country_currency_mapping.items() if value == currency)
            rfr_row = rfr_values[rfr_values['Unnamed: 1'].str.startswith(country_code)]
        
        if rfr_row.empty:
            raise ValueError(f"No spot values found for currency {currency}")

        if years < 1:
            return rfr_row[1].values[0]
        elif years > 150:
            return rfr_row[150].values[0]
        else:
            lower_year = int(np.floor(years))
            upper_year = int(np.ceil(years))
            lower_value = rfr_row[int(lower_year)].values[0]
            upper_value = rfr_row[int(upper_year)].values[0]
            return lower_value + (upper_value - lower_value) * (years - lower_year)

    filtered_data['rfr_no_va_value'] = filtered_data.apply(lambda row: interpolate_rfr(row, rfr_no_va), axis=1)
    filtered_data['up_shock_rate_value'] = filtered_data.apply(lambda row: interpolate_rfr(row, up_shock_rates), axis=1)
    filtered_data['down_shock_rate_value'] = filtered_data.apply(lambda row: interpolate_rfr(row, down_shock_rates), axis=1)

    # Calculate the shock values
    filtered_data['up_shock_rfr_value'] = filtered_data['up_shock_rate_value'] - filtered_data['rfr_no_va_value']
    filtered_data['down_shock_rfr_value'] = filtered_data['down_shock_rate_value'] - filtered_data['rfr_no_va_value']

    filtered_data['up_shock_value'] = - filtered_data['value'] * filtered_data['up_shock_rfr_value'] * filtered_data['mod_dur_of_underlying']
    filtered_data['down_shock_value'] = - filtered_data['value'] * filtered_data['down_shock_rfr_value'] * filtered_data['mod_dur_of_underlying']

    filtered_data['up_shock_market_value'] = filtered_data['value'] + filtered_data['up_shock_value']
    filtered_data['down_shock_market_value'] = filtered_data['value'] + filtered_data['down_shock_value']

    #Dataframe kimentéséhez átalakítjuk kicsit
    output_interest_rate = filtered_data.groupby(['portfolio_id', 'portfolio_group']).agg({
    'security_name': 'first',
    'qc_of_underlying': lambda x: filtered_data.loc[x.index].groupby('qc_of_underlying')['value'].sum().idxmax(),
    'years_to_maturity': lambda x: x.iloc[0] if len(x.unique()) == 1 else '',
    'mod_dur_of_underlying': lambda x: x.iloc[0] if len(x.unique()) == 1 else '',
    'value': 'sum',
    'up_shock_value': 'sum',
    'down_shock_value': 'sum',
    'up_shock_market_value': 'sum',
    'down_shock_market_value': 'sum'
    }).reset_index()

    #Oszlopok sorbarendezése
    output_interest_rate = output_interest_rate[[
        'portfolio_group', 
        'portfolio_id', 
        'security_name', 
        'qc_of_underlying',
        'years_to_maturity',
        'mod_dur_of_underlying',
        'value',
        'down_shock_market_value',
        'down_shock_value',
        'up_shock_market_value',
        'up_shock_value'
    ]]

    return output_interest_rate

def calculate_spread_risk(data, param_file, calculation_matrix, calculation_type_mapping):
    # Read the parameters from the JSON file
    with open(param_file, 'r') as file:
        parameters = json.load(file)
    bond_rated_a_i_matrix = parameters['spread_risk_matrices']['bond_rated_a_i_matrix']
    bond_rated_b_i_matrix = parameters['spread_risk_matrices']['bond_rated_b_i_matrix']
    bond_unrated_a_i_matrix = parameters['spread_risk_matrices']['bond_unrated_a_i_matrix']
    bond_unrated_b_i_matrix = parameters['spread_risk_matrices']['bond_unrated_b_i_matrix']

    #Megszűrjpk a dataframet
    spread_risk_types = [key for key, value in calculation_type_mapping.items() if calculation_matrix.iloc[value]['Spread Risk'] == 'x'] + ['1X']
    filtered_data = data[data['calculation_type'].isin(spread_risk_types)].copy()

    def transform_cqs(cqs):
        valid_values = {0, 1, 2, 3, 4, 5, 6, 'NR'}
        if cqs == 'NR':
            return 'NR'
        if cqs > 6:
            return 'NR'
        elif cqs not in valid_values:
            raise ValueError(f"Invalid cqs value: {cqs}. Must be one of {valid_values}.")
        return int(cqs)
    
    filtered_data['cqs'] = filtered_data['cqs'].apply(transform_cqs)

    #Megfelelő modified durationhoz megkeressük a megfelelő mátrix sort
    #Ha kevesebb mint 1, akkor 1 lesz
    def get_matrix_row_number(duration):
        if duration < 1:
            return 0
        elif duration < 5:
            return 0
        elif duration < 10:
            return 1
        elif duration < 15:
            return 2
        elif duration < 20:
            return 3
        else:
            return 4
        
    def adjust_duration(duration):
        if duration < 1:
            return 1
        elif duration < 5:
            return duration
        elif duration < 10:
            return duration - 5
        elif duration < 15:
            return duration - 10
        elif duration < 20:
            return duration - 15
        else:
            return duration - 20

    filtered_data['matrix_row_number'] = filtered_data['mod_dur_of_underlying'].apply(get_matrix_row_number)
    filtered_data['mod_dur_of_underlying_'] = filtered_data['mod_dur_of_underlying'].apply(adjust_duration)

    # Calculate shocked values
    def calculate_shocked_value(row):
        if row['cqs'] == 'NR':
            i = row['matrix_row_number']
            j = 0  #Unrated-re mindig 1 oszlop van csak
            a_i = bond_unrated_a_i_matrix[i][j]
            b_i = bond_unrated_b_i_matrix[i][j]
            dur_i = row['mod_dur_of_underlying_']
            return row['value'] * (1 - min((a_i + b_i * dur_i), 1))
        else:
            i = row['matrix_row_number']
            j = int(row['cqs'])
            if j == 6:
                j = 4
            a_i = bond_rated_a_i_matrix[i][j]
            b_i = bond_rated_b_i_matrix[i][j]
            dur_i = row['mod_dur_of_underlying_']
            return row['value'] * (1 - min((a_i + b_i * dur_i), 1))

    filtered_data['spread_shocked_mv_value'] = filtered_data.apply(calculate_shocked_value, axis=1)
    filtered_data['spread_shock_value'] = filtered_data['value'] - filtered_data['spread_shocked_mv_value']

    #Dataframe kimentéséhez átalakítjuk kicsit
    output_spread = filtered_data.groupby(['portfolio_id', 'portfolio_group']).agg({
    'security_name': 'first',
    'cqs': lambda x: x.iloc[0] if len(x.unique()) == 1 else '',
    'mod_dur_of_underlying': lambda x: x.iloc[0] if len(x.unique()) == 1 else '',
    'value': 'sum',
    'spread_shocked_mv_value': 'sum',
    'spread_shock_value': 'sum'
    }).reset_index()

    #Oszlopok sorbarendezése
    output_spread = output_spread[[
        'portfolio_group', 
        'portfolio_id', 
        'security_name',
        'cqs', 
        'mod_dur_of_underlying',
        'value',
        'spread_shocked_mv_value',
        'spread_shock_value'
    ]]

    return output_spread

def calculate_currency_risk(data, param_file, exchange_rates_file, calculation_matrix, calculation_type_mapping):
    #JSON fileból beolvassuk a plusz paramétereket
    with open(param_file, 'r') as file:
        parameters = json.load(file)
    currency_up_shock = parameters['currency_risk']['currency_up_shock']
    currency_down_shock = parameters['currency_risk']['currency_down_shock']

    exchange_rates = pd.read_excel(exchange_rates_file, sheet_name=0, header=0, usecols=lambda x: x not in ['Dátum/ISO', 'HUF'])

    last_row = exchange_rates.iloc[-1]
    units_row = exchange_rates.iloc[0]

    exchange_rate_dict = {currency: last_row[currency] * units_row[currency] for currency in last_row.index if pd.notna(last_row[currency])}

    #Megszűrjük az adatokat
    currency_risk_types = [key for key, value in calculation_type_mapping.items() if calculation_matrix.iloc[value]['Concentration Risk'] == 'x'] + ['1X', '3S', 'FX']
    filtered_data = data[data['calculation_type'].isin(currency_risk_types)].copy()

    fx_data = filtered_data[filtered_data['calculation_type'] == 'FX'].copy()
    non_fx_data = filtered_data[filtered_data['calculation_type'] != 'FX'].copy()

    #Csoportosítás
    grouped_data = non_fx_data.groupby(['portfolio_id', 'qc_of_underlying']).agg({
        'portfolio_group': 'first',
        'security_name': 'first',
        'balance_nominal': lambda x: x.iloc[0] if len(x.unique()) == 1 else '',
        'value': 'sum'
    }).reset_index()

    #Shockok alkalmazása
    grouped_data['up_shock_mv_value'] = grouped_data.apply(
        lambda row: row['value'] * (1 + currency_up_shock) if row['qc_of_underlying'] != 'HUF' else row['value'], axis=1
    )
    grouped_data['up_shock_value'] = grouped_data['value'] - grouped_data['up_shock_mv_value']

    grouped_data['down_shock_mv_value'] = grouped_data.apply(
        lambda row: row['value'] * (1 - currency_down_shock) if row['qc_of_underlying'] != 'HUF' else row['value'], axis=1
    )
    grouped_data['down_shock_value'] = grouped_data['value'] - grouped_data['down_shock_mv_value']

    #Külön az opciók
    fx_data['down_shock_mv_value'] = fx_data.apply(
        lambda row: row['balance_nominal'] * exchange_rate_dict.get(row['qc_of_underlying'], row['value']), axis=1
    )
    fx_data['down_shock_value'] = fx_data['value'] - fx_data['down_shock_mv_value']

    fx_data['up_shock_mv_value'] = 0
    fx_data['up_shock_value'] = fx_data['value']

    #Csak a szükséges oszlopok
    fx_data = fx_data[[
        'portfolio_group',
        'portfolio_id',
        'security_name', 
        'qc_of_underlying', 
        'value',
        'down_shock_mv_value',
        'down_shock_value',
        'up_shock_mv_value',
        'up_shock_value'
    ]]

    #Eredmények összetétele
    result_df = pd.concat([grouped_data, fx_data], ignore_index=True)

    #Oszlopok sorbarendezése
    output_currency = result_df[[
        'portfolio_group', 
        'portfolio_id', 
        'security_name',
        'qc_of_underlying',
        'value',
        'down_shock_mv_value',
        'down_shock_value',
        'up_shock_mv_value',
        'up_shock_value'
    ]]

    return output_currency

def calculate_property_risk(data, param_file, calculation_matrix, calculation_type_mapping):
    # Read the parameters from the JSON file
    with open(param_file, 'r') as file:
        parameters = json.load(file)
    property_shock = parameters['property_risk']['property_shock']

    #Megszűrjük a dataframet
    property_risk_types = [key for key, value in calculation_type_mapping.items() if calculation_matrix.iloc[value]['Property Risk'] == 'x']
    filtered_data = data[data['calculation_type'].isin(property_risk_types)].copy()

    filtered_data['property_shock_value'] = filtered_data['value'] * property_shock
    filtered_data['property_shocked_mv_value'] = filtered_data['value'] - filtered_data['property_shock_value']

    #Dataframe kimentéséhez átalakítjuk kicsit
    output_property = filtered_data.groupby(['portfolio_id', 'portfolio_group']).agg({
    'security_name': 'first',
    'value': 'sum',
    'property_shocked_mv_value': 'sum',
    'property_shock_value': 'sum'
    }).reset_index()

    #Oszlopok sorbarendezése
    output_property = output_property[[
        'portfolio_group', 
        'portfolio_id', 
        'security_name',
        'value',
        'property_shocked_mv_value',
        'property_shock_value'
    ]]

    return output_property

def calculate_concentration_risk(data, param_file, calculation_matrix, calculation_type_mapping):
    #JSON fileból beolvassuk a plusz paramétereket
    with open(param_file, 'r') as file:
        parameters = json.load(file)
    risk_factors = parameters['concentration_risk']['risk_factors']
    gov_risk_factors = parameters['concentration_risk']['other_gov_bonds_risk_factors']
    excess_exposure_thresholds = parameters['concentration_risk']['excess_exposure_thresholds']
    tolerance = parameters['concentration_risk']['tolerance']
    country_currency_mapping = parameters['COUNTRY_CURRENCY_MAPPING']

    # Filter the data for relevant calculation types
    concentration_risk_types = [key for key, value in calculation_type_mapping.items() if calculation_matrix.iloc[value]['Concentration Risk'] == 'x'] + ['1X', '3S']
    filtered_data = data[data['calculation_type'].isin(concentration_risk_types)].copy()

    filtered_data['cqs'] = filtered_data['cqs'].replace('NR', 6).fillna(6).astype(float)
    filtered_data['value'] = filtered_data['value'].fillna(0).astype(float)
    filtered_data['id_of_underlying'] = filtered_data['id_of_underlying'].fillna('dummy_id')

    # Calculate the total portfolio value
    total_portfolio_value = data.loc[~data['asset_allocation'].str.startswith(('995', '645'))]['value'].sum()

    # Calculate the weighted CQS for each portfolio_id
    filtered_data['weighted_cqs_value'] = filtered_data['cqs'] * filtered_data['value']
    weighted_cqs = filtered_data.groupby(['portfolio_id', 'id_of_underlying']).apply(
        lambda x: int(np.ceil((x['weighted_cqs_value'].sum() / x['value'].sum()) - tolerance)) if x['value'].sum() != 0 else 0
    ).reset_index(name='weighted_cqs')

    # Aggregate the data by portfolio_id
    portfolio_aggregated = filtered_data.groupby(['portfolio_id', 'id_of_underlying']).agg({
        'portfolio_group': 'first',
        'security_name': 'first',
        'currency': 'first',
        'country': 'first',
        'calculation_type': lambda x: (x == '1X').all(),
        'value': 'sum'
    }).reset_index()

    # Merge the weighted CQS back into the aggregated data
    portfolio_aggregated = portfolio_aggregated.merge(weighted_cqs, on=['portfolio_id', 'id_of_underlying'])

    # Calculate the threshold value for each portfolio_id based on weighted_cqs
    def get_threshold(row):
        cqs_index = min(int(row['weighted_cqs']), len(excess_exposure_thresholds) - 1)
        return excess_exposure_thresholds[cqs_index]

    portfolio_aggregated['threshold'] = portfolio_aggregated.apply(get_threshold, axis=1)
    portfolio_aggregated['threshold_value'] = portfolio_aggregated['threshold'] * total_portfolio_value

    # Calculate the excess exposure for each portfolio_id
    portfolio_aggregated['excess_exposure'] = portfolio_aggregated['value'] - portfolio_aggregated['threshold_value']
    portfolio_aggregated['excess_exposure'] = portfolio_aggregated['excess_exposure'].apply(lambda x: max(0, x))

    # Determine the risk factor for each portfolio_id
    def get_risk_factor(row):
        if row['calculation_type'] and row['currency'] == country_currency_mapping.get(row['country'], ''):
            return gov_risk_factors[min(int(row['weighted_cqs']), len(gov_risk_factors) - 1)]
        else:
            return risk_factors[min(int(row['weighted_cqs']), len(risk_factors) - 1)]

    portfolio_aggregated['risk_factor'] = portfolio_aggregated.apply(get_risk_factor, axis=1)

    # Calculate the concentration risk
    portfolio_aggregated['concentration_risk'] = portfolio_aggregated['excess_exposure'] * portfolio_aggregated['risk_factor']
    
    output_concentration = portfolio_aggregated.groupby('portfolio_id').agg({
        'portfolio_group': 'first',
        'security_name': 'first',
        'currency': 'first',
        'country': 'first',
        'weighted_cqs': lambda x: x.iloc[0] if len(x.unique()) == 1 else '',
        'threshold': lambda x: x.iloc[0] if len(x.unique()) == 1 else '',
        'threshold_value': lambda x: x.iloc[0] if len(x.unique()) == 1 else '',
        'excess_exposure': 'sum',
        'risk_factor': lambda x: x.iloc[0] if len(x.unique()) == 1 else '',
        'value': 'sum',
        'concentration_risk': 'sum'
    }).reset_index()

    output_concentration = output_concentration[[
        'portfolio_group', 
        'portfolio_id', 
        'security_name',
        'currency',
        'country',
        'weighted_cqs',
        'threshold',
        'threshold_value',
        'excess_exposure',
        'risk_factor',
        'value',
        'concentration_risk'
    ]]

    return output_concentration

def calculate_years_to_maturity(row):
    """
    Számoljuk ki a years to maturity-t pontosan két dátum között
    """
    start_date = row['analysis_date']
    end_date = row['maturity_date']
    
    # Calculate the difference in years
    years_difference = end_date.year - start_date.year
    
    # Calculate the remaining days difference
    start_of_end_year = datetime(end_date.year, start_date.month, start_date.day)
    if end_date < start_of_end_year:
        start_of_end_year = datetime(end_date.year - 1, start_date.month, start_date.day)
        years_difference -= 1
    
    days_difference = (end_date - start_of_end_year).days
    
    # Calculate the total years to maturity
    total_years = years_difference + days_difference / 365.2425
    return total_years