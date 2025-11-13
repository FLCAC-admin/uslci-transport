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

#%% Code Mapping
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

#%% Add values for inputs ###

from flcac_utils.mapping import prepare_tech_flow_mappings

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
df_olca['default_provider'] = df_olca['FlowName'].map(
    {k: v.id for k, v in provider_dict.items()})


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
    'Context': f'Technosphere Flows / {meta.get("Category")}',
    'IsInput': False,
    'FlowType':'PRODUCT_FLOW',
    'reference': True,
    'default_provider': 'nan',
    'default_provider_name': 'nan',
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


#%% Add values shared by both inputs and ref flow

df_olca['ProcessCategory'] = f'{meta.get("Category")}'
df_olca['Context'] = 'Technosphere Flows / 48-49: Transportation and Warehousing'
## TODO: ^^ input flows need to have specific context see #4
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
            _df_olca['FlowName'] == 'Commodity transport; at consumer', 'exchange_dqi'] = source_row['exchange_dqi']
    
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
        )
        processes.update(p_dict)


out_path = working_dir / 'output'
write_objects('uslci-transport', flows, new_flows, processes,
              location_objs, source_objs, actor_objs, dq_objs,
              out_path = out_path
              )