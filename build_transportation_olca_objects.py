"""
The purpose of this script is to build olca process objects that will contain
the 2017 CFS PUF derived transport data for commodities defined according to
the 2017 Standard Classification of Transported Goods (SCTG) Codes.

The distances associated with each transport mode within a commodity category
are weighted by the total moass of that commodity transported via the
respective transportation mode.

This script only works after running 'commodity transport distances.py'
"""


#%% SETUP ##

## DEPENDENCIES ##
import pandas as pd
from pathlib import Path
import yaml
from esupy.util import make_uuid
import copy
from typing import Dict
import numpy as np

# Directories
working_dir = Path(__file__).parent # parent directory
data_dir = working_dir / 'data' # data directory
meta_dir = working_dir / 'metadata' # metadata directory

# Load yaml file for flow meta data
with open(meta_dir / 'transport_flow_meta.yaml') as f:
    meta = yaml.safe_load(f)

# Read in CSV file created by 'commodity transport distances.py'
csv_path = data_dir / 'Weighted_Commodity_Transport_Distances.csv'
df_olca = pd.read_csv(csv_path)
df_olca = df_olca.drop(columns=['Mass Shipped (kg)', 'Avg. Dist. Shipped (km)', 'Mass Frac. by Mode'])
YEAR = 2017

# Create empty df_olca that includes all schema requirements
schema = ['ProcessID',
          'ProcessCategory',
          'ProcessName',
          'FlowUUID', 
          'FlowName',
          'amountFormula',
          'Context',
          'IsInput', 
          'FlowType', 
          'reference', 
          'default_provider',
          'default_provider_name',
          'amount',
          'unit',
          'avoided_product',
          'exchange_dqi',
          'location']

# Add schema columns to df_olca
for column in schema:
    df_olca[column] = ''
    
# Move values from 'Weighted Dist. Shipped (km)' to 'amount'
# Remove 'Weighted Dist. Shipped (km)' column
df_olca['amount'] = df_olca['Weighted Dist. Shipped (km)']
df_olca = df_olca.drop('Weighted Dist. Shipped (km)', axis=1)
                                                                                                                                                   

#%% SCTG Mapping
SCTG_codes = {
    '01': 'Animals and Fish (live)',
    '02': 'Cereal Grains (includes seed)',
    '03': 'Agricultural Products (excludes Animal Feed, Cereal Grains, and Forage Products)',
    '04': 'Animal Feed, Eggs, Honey, and Other Products of Animal Origin',
    '05': 'Meat, Poultry, Fish, Seafood, and Their Preparations',
    '06': 'Milled Grain Products and Preparations, and Bakery Products',
    '07': 'Other Prepared Foodstuffs, and Fats and Oils',
    '08': 'Alcoholic Beverages and Denatured Alcohol',
    '09': 'Tobacco Products',
    '10': 'Monumental or Building Stone',
    '11': 'Natural Sands',
    '12': 'Gravel and Crushed Stone (excludes Dolomite and Slate)',
    '13': 'Other Non-Metallic Minerals not elsewhere classified',
    '14': 'Metallic Ores and Concentrates',
    '15': 'Coal',
    '16': 'Crude Petroleum',
    '17': 'Gasoline, Aviation Turbine Fuel, and Ethanol (includes Kerosene, and Fuel Alcohols)',
    '18': 'Fuel Oils (includes Diesel, Bunker C, and Biodiesel)',
    '19': 'Other Coal and Petroleum Products, not elsewhere classified',
    '20': 'Basic Chemicals',
    '21': 'Pharmaceutical Products',
    '22': 'Fertilizers',
    '23': 'Other Chemical Products and Preparations',
    '24': 'Plastics and Rubber',
    '25': 'Logs and Other Wood in the Rough',
    '26': 'Wood Products',
    '27': 'Pulp, Newsprint, Paper, and Paperboard',
    '28': 'Paper or Paperboard Articles',
    '29': 'Printed Products',
    '30': 'Textiles, Leather, and Articles of Textiles or Leather',
    '31': 'Non-Metallic Mineral Products',
    '32': 'Base Metal in Primary or Semi-Finished Forms and in Finished Basic Shapes',
    '33': 'Articles of Base Metal',
    '34': 'Machinery',
    '35': 'Electronic and Other Electrical Equipment and Components, and Office Equipment',
    '36': 'Motorized and Other Vehicles (includes parts)',
    '37': 'Transportation Equipment, not elsewhere classified',
    '38': 'Precision Instruments and Apparatus',
    '39': 'Furniture, Mattresses and Mattress Supports, Lamps, Lighting Fittings, and Illuminated Signs',
    '40': 'Miscellaneous Manufactured Products',
    '41': 'Waste and Scrap (excludes of agriculture or food, see 041xx)',
    '43': 'Mixed Freight'
}

# Reverse SCTG_codes
reversed_SCTG_codes = {v: k for k, v in SCTG_codes.items()}

# Add SCTG codes to df_olca
df_olca['SCTG'] = df_olca['Commodity'].map(reversed_SCTG_codes)

# Reorder for comparison
df_olca = df_olca[['SCTG'] + [col for col in df_olca.columns if col != 'SCTG']]

# Check if any SCTG not in df
missing_keys = set(SCTG_codes.keys()) - set(df_olca['SCTG'])
if len(missing_keys) > 0:
    for k in missing_keys:
        print(f'Missing SCTG code: {SCTG_codes[k]} ({k})')


#%% Build df_params

param_map = {
    'company-owned truck': 'lightTruck',
    'for-hire truck':      'longHaul',
    'rail':                'rail',
    'great lakes':         'greatLakes',
    'inland water':        'inlandWater',
    'air(incl. truck & air)': 'air',
    'deep sea':            'deepSea',
    'pipeline':            'pipeline',
}


def build_df_params(df_commData: pd.DataFrame, param_map: Dict[str, str]) -> pd.DataFrame:
    """
    Build df_params with mode-centric parameter names (no SCTG embedded).

    What this does:
    - Creates three rows per transport mode for each commodity:
      1) dist (input)           -> <modeKey>_dist
      2) mass_frac (input)      -> <modeKey>_mass_frac
      3) kgkm (derived, formula)-> <modeKey>_kgkm
    - Writes the final table to ./data/uslci_transport_params.csv.
    - Returns df_params as a DataFrame.
    """
    def _normalize_mode(s: str) -> str:
        # Normalize raw mode labels from the CSV for stable matching to param_map keys
        s = str(s).replace("&amp;amp;amp;amp;", "&amp;amp;amp;").strip().lower()
        # Collapse any accidental double spaces
        while "  " in s:
            s = s.replace("  ", " ")
        return s
    # Map normalized mode labels (CSV) -> mode keys (used in parameter names)
    norm_param_map = {_normalize_mode(k): v for k, v in param_map.items()}
    def _process_name(commodity: str) -> str:
        # Process scoping: parameter names are mode-centric, so uniqueness comes from processName
        return f"Transport; average mix; {commodity.strip().lower()}"
    def _make_param_names(mode_key: str):
        # Construct the three parameter names for a given transport mode
        return f"{mode_key}_dist", f"{mode_key}_mass_frac", f"{mode_key}_kgkm"
    # Target output schema
    cols = ["processName", "formula", "isInputParameter", "name", "value", "description"]
    rows = []
    # Build rows: one triplet per (commodity, transport mode)
    for idx, row in df_commData.iterrows():
        commodity = row["Commodity"]
        mode_norm = _normalize_mode(row["Transport Mode"])
        # Fail fast if a mode from the CSV is not present in the mapping
        if mode_norm not in norm_param_map:
            raise KeyError(f"Mode '{row['Transport Mode']}' not in param_map (row {idx})")
        mode_key = norm_param_map[mode_norm]
        pname = _process_name(commodity)
        # Source values from the CSV
        dist_val = row["Avg. Dist. Shipped (km)"]
        mass_frac_val = row["Mass Frac. by Mode"]
        # Parameter names and derived formula
        name_dist, name_mf, name_kgkm = _make_param_names(mode_key)
        formula_kgkm = f"{name_dist}*{name_mf}"
        # Two inputs (dist, mass_frac) and one derived (kgkm)
        rows.append([pname, "", "TRUE", name_dist, dist_val, "km"])
        rows.append([pname, "", "TRUE", name_mf, mass_frac_val,
                     "fraction of all commodity shipped by current mode"])
        rows.append([pname, formula_kgkm, "FALSE", name_kgkm, np.nan,
                     "kg*km; average mass distance per shipment"])
    # Assemble the final table
    return pd.DataFrame(rows, columns=cols)


df_params = build_df_params(pd.read_csv(csv_path), param_map)
df_params.to_csv(data_dir / "uslci_transport_params.csv", index=False)


#%% Add values for inputs ###

from flcac_utils.mapping import prepare_tech_flow_mappings

def assign_amount_formula(
    df_olca: pd.DataFrame,
    df_param: pd.DataFrame,
    param_map: dict) -> pd.DataFrame:
    """
    Populate 'amountFormula' with the '<modeKey>_kgkm' symbol based on ProcessName and Transport Mode.
    Works with the new mode-centric parameter names (no SCTG in names).
    """
    def _norm_mode(s: str) -> str:
        return str(s).strip().lower().replace("&amp;amp;amp;", "&amp;amp;").replace("  ", " ")
    # Minimal change: map normalized mode labels directly to mode keys
    mode_to_token = {_norm_mode(k): v for k, v in param_map.items()}
    # Reduce df_param to *_kgkm rows and extract mode token from the new names
    dfp = df_param[df_param["name"].str.endswith("_kgkm")].copy()
    # Option B (recommended): strip the suffix to get the mode_key directly
    dfp["mode_token"] = dfp["name"].str.replace(r"_kgkm$", "", regex=True)
    # Build lookup keyed by (processName, mode_token) -> '<modeKey>_kgkm'
    lookup = dfp.set_index(["processName", "mode_token"])["name"]  # Series
    # Compute mode token in df_olca and assign
    df_new = df_olca.copy()
    df_new["_mode_norm"] = df_new["Transport Mode"].map(_norm_mode)
    df_new["_mode_token"] = df_new["_mode_norm"].map(mode_to_token)

    keys = list(zip(df_new["ProcessName"], df_new["_mode_token"]))
    df_new["amountFormula"] = pd.Series(keys).map(lookup.to_dict())

    df_new.drop(columns=["_mode_norm", "_mode_token"], inplace=True)
    return df_new

# Asign static values
df_olca['IsInput'] = True
df_olca['reference'] = False
df_olca['unit'] = 'kg*km'
df_olca['ProcessName'] = 'Transport; average mix; ' + df_olca['Commodity'].str.lower()
df_olca['ProcessID'] = df_olca['ProcessName'].apply(make_uuid)

# Extract data from the transport flow mapping file and apply to data frame
flow_dict, flow_objs, provider_dict = prepare_tech_flow_mappings(
    pd.read_csv(data_dir / 'transport_mapping.csv'))

# Map flow name based on transport mode mapping to uslci
df_olca['FlowName'] = df_olca['Transport Mode'].map(
    {k: v['name'] for k, v in flow_dict.items()})

# Map flow uuid based on transport mode mapping to uslci
df_olca['FlowUUID'] = df_olca['Transport Mode'].map(
    {k: v['id'] for k, v in flow_dict.items()})

# Map default provider name based on transport mode mapping to uslci
df_olca['default_provider_name'] = df_olca['Transport Mode'].map(
    {k: v['provider'] for k, v in flow_dict.items()})

# Map default provider uuid based on mapped flow name
df_olca['default_provider'] = df_olca['default_provider_name'].map(
    {k: v.id for k, v in provider_dict.items()})

# Assign amount formula 
df_olca = assign_amount_formula(df_olca, df_params, param_map)

#%% Create ref flow df that will be updated for each process ###

# Create FlowName by modifying the commodity string
refFlowName = 'Commodity transport; at consumer'

# generate reference flow uuid
refFlowUUID = make_uuid([refFlowName, 'uslci-transport'])

# Dictionary of ref flow values
ref_flow = {
    'SCTG': 'nan',
    'Commodity': 'nan',
    'Transport Mode': 'nan',
    'ProcessID': 'nan', # Updated for each process in create json file loop
    'ProcessCategory': f'{meta.get("Category")}',
    'ProcessName': 'nan', # Updated for each process in create json file loop
    'FlowUUID': refFlowUUID,
    'FlowName': refFlowName,
    'amountFormula':'nan', # this will not be added when make_exchanges() is called unless overwritten with parameter name
    'Context': f'Technosphere flows / {meta.get("Category")}',
    'IsInput': False,
    'FlowType':'PRODUCT_FLOW',
    'reference': True,
    'default_provider': '',
    'default_provider_name': '',
    'amount': 1.0,
    'unit': 'kg',
    'avoided_product': False,
    'exchange_dqi': 'nan', # Updated for each process in create json file loop
    'location': 'US',
    'Year': YEAR,
    'CountryCode': 'USA'
}

# Convert ref flow data to df; will be concatenated in the create json file loop
refFlow_df = pd.DataFrame([ref_flow])

# Assign parameter to reference flow if parameter is used for exchange value
#refFlow_df['amountFormula'] = 


#%% Add values shared by both inputs and ref flow

df_olca['ProcessCategory'] = f'{meta.get("Category")}'
df_olca['Context'] = 'Technosphere flows / 48-49: Transportation and Warehousing'
##^^ input flows are replaced via API so context does not matter see #4
df_olca['FlowType'] = 'PRODUCT_FLOW'
df_olca['avoided_product'] = False
df_olca['location'] = 'US'
df_olca['Year'] = YEAR


#%% Assign exchange dqi

from flcac_utils.util import format_dqi_score
df_olca['exchange_dqi'] = format_dqi_score(meta['DQI']['Flow'])


#%% Assign locations to processes

from flcac_utils.util import generate_locations_from_exchange_df
from esupy.location import read_iso_3166
df_olca = df_olca.merge(read_iso_3166()
                            .filter(['ISO-2d', 'ISO-3d'])
                            .rename(columns={'ISO-3d': 'CountryCode',
                                             'ISO-2d': 'location'}),
                        how='left')
locations = generate_locations_from_exchange_df(df_olca)


#%% Build supporting objects

from flcac_utils.generate_processes import build_location_dict
from flcac_utils.util import extract_actors_from_process_meta, \
    extract_sources_from_process_meta, extract_dqsystems, assign_year_to_meta

# Load yaml file for process meta data
with open(meta_dir / 'transport_process_meta.yaml') as f:
    process_meta = yaml.safe_load(f)

(process_meta, source_objs) = extract_sources_from_process_meta(
    process_meta, bib_path = meta_dir / 'transport_sources.bib')

(process_meta, actor_objs) = extract_actors_from_process_meta(process_meta)

dq_objs = extract_dqsystems(meta['DQI']['dqSystem'])

process_meta['dq_entry'] = format_dqi_score(meta['DQI']['Process'])
process_meta = assign_year_to_meta(process_meta, YEAR)

# generate dictionary of location objects
location_objs = build_location_dict(df_olca, locations)


#%% Create json file

from flcac_utils.generate_processes import build_flow_dict, \
    build_process_dict, write_objects, validate_exchange_data

# Add ref flow so the new flow gets created
df_olca = pd.concat([df_olca, refFlow_df], ignore_index=True)

validate_exchange_data(df_olca)
# Need to update this so that the new ref flow gets created
flows, new_flows = build_flow_dict(df_olca)
# replace newly created flows with those pulled via API
api_flows = {flow.id: flow for k, flow in flow_objs.items()}
if not(flows.keys() | api_flows.keys()) == flows.keys():
    print('Warning, some flows not consistent')
else:
    flows.update(api_flows)
processes = {}
# Loop over each unique ProcessID
for pid in df_olca['ProcessID'].unique():
    if pid != 'nan':
        # Create a fresh reference flow row for this pid
        ref_flow_copy = refFlow_df.copy()
        ref_flow_copy['ProcessID'] = pid

        # Filter the DataFrame for the current ProcessID
        _df_olca = pd.concat([
            df_olca[df_olca['ProcessID'] == pid],
            ref_flow_copy
        ], ignore_index=True)

        # Get a donor row to provide current process name and dqi
        source_row = _df_olca[_df_olca['FlowName'] != 'Commodity transport; at consumer'].iloc[0]

        # Update reference flow with current process name and dqi
        _df_olca.loc[
            _df_olca['FlowName'] == 'Commodity transport; at consumer', 'ProcessName'] = source_row['ProcessName']
        _df_olca.loc[
            _df_olca['FlowName'] == 'Commodity transport; at consumer', 'exchange_dqi'] = ''
    
        # Get representative values for replacement (e.g., first row)
        commodity = _df_olca.iloc[0]['Commodity']
        sctg = _df_olca.iloc[0]['SCTG']
    
        # Create a deep copy of process_meta
        _process_meta = copy.deepcopy(process_meta)
    
        # Replace placeholders in all string values of _process_meta
        for key, value in _process_meta.items():
            if isinstance(value, str):
                _process_meta[key] = value.replace('[COMMODITY]', commodity).replace('[SCTG]', sctg)
            
        # Now call your function with filtered data
        p_dict = build_process_dict(
            _df_olca,
            flows,
            meta=_process_meta,
            loc_objs=location_objs,
            source_objs=source_objs,
            actor_objs=actor_objs,
            dq_objs=dq_objs,
            df_params = df_params
        )
        processes.update(p_dict)


out_path = working_dir / 'output'
write_objects('uslci-transport', flows, new_flows, processes,
              location_objs, source_objs, actor_objs, dq_objs,
              out_path = out_path
              )

#%% Unzip files to repo
from flcac_utils.util import extract_latest_zip
extract_latest_zip(out_path,
                   working_dir,
                   output_folder_name = out_path / 'uslci-transport_v1.0')
