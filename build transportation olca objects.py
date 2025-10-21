#%% PURPOSE ##

'''

The purpose of this script is to build olca process objects that will contain
the 2017 CFS PUF derived transport data for commodities defined according to
the 2017 Standard Classification of Transported Goods (SCTG) Codes.

The distances associated with each transport mode within a commodity category
are weighted by the total moass of that commodity transported via the
respective transportation mode.

This script only works after running 'commodity transport distances.py'

TO-DO:
- Ask questions 
- Complete dqi in flow meta
- fill out process meta


'''


#%% SETUP ##

## DEPENDENCIES ##
import pandas as pd
import numpy as np
from pathlib import Path
import yaml
from esupy.util import make_uuid
import os

# working directory
working_dir = Path(r'C:\Users\mchristie\Code\uslci-transport')

# Load yaml file for flow meta data
with open(working_dir / 'transport_flow_meta.yaml') as f:
    meta = yaml.safe_load(f)

# Load yaml file for process meta data
with open(working_dir / 'transport_process_meta.yaml') as f:
    process_meta = yaml.safe_load(f)

# Read in CSV file created by 'commodity transport distances.py'
csv_path = r'C:\Users\mchristie\Code\uslci-transport\Weighted Commodity Transport Distances.csv'
df_olca = pd.read_csv(csv_path)
df_olca = df_olca.drop(columns=['Mass Shipped (kg)', 'Avg. Dist. Shipped (km)', 'Mass Frac. by Mode'])

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
df_olca.drop('Weighted Dist. Shipped (km)', axis=1, inplace=True)


#%% Add values for inputs ###
df_olca['IsInput'] = True
df_olca['reference'] = False
df_olca['unit'] = 'kg*km'
df_olca['ProcessName'] = 'Transport, average, ' + df_olca['Commodity'].str.lower()
df_olca['ProcessID'] = df_olca['ProcessName'].apply(make_uuid)

# Map flow name based on transport mode mapping to uslci in transport_flow_meta.yaml
df_olca['FlowName'] = df_olca['Transport Mode'].map(
    {k: v['FlowName'] for k, v in meta['Mode'].items()})

# Map flow uuid based on transport mode mapping to uslci in transport_flow_meta.yaml
df_olca['FlowUUID'] = df_olca['Transport Mode'].map(
    {k: v['FlowUUID'] for k, v in meta['Mode'].items()})

# Map default provider name based on transport mode mapping to uslci in transport_flow_meta.yaml
df_olca['default_provider_name'] = df_olca['Transport Mode'].map(
    {k: v['ProcessName'] for k, v in meta['Mode'].items()})

# Map default provider uuid based on transport mode mapping to uslci in transport_flow_meta.yaml
df_olca['default_provider'] = df_olca['Transport Mode'].map(
    {k: v['DefaultProviderUUID'] for k, v in meta['Mode'].items()})


#%% Create new flows for the reference flow of each process ###

# Get unique commodities
unique_commodities = df_olca['Commodity'].unique()

# Creat list for dictionaries of ref flow values
new_rows = []
for commodity in unique_commodities:
    
    # Get process uuid and name for each new ref flow
    processID = df_olca[df_olca['Commodity'] == commodity]['ProcessID'].iloc[0]
    processName = df_olca[df_olca['Commodity'] == commodity]['ProcessName'].iloc[0]
    
    # Create FlowName by modifying the commodity string
    flowName = f"{commodity}, transported"
    
    # generate reference flow uuid
    flowUUID = make_uuid([flowName, processName, processID])

    # Create the new row as a dictionary
    new_row = {
        'Commodity': commodity,
        'ProcessID': processID,
        'ProcessName': processName,
        'FlowName': flowName,
        'FlowUUID': flowUUID,
        'IsInput': False,
        'reference': True,
        'amount': 1.0,
        'unit': 'kg',
        'default_provider': 'nan',
        'default_provider_name': 'nan'
    }
    new_rows.append(new_row)

# Convert new rows to DataFrame
new_df = pd.DataFrame(new_rows)

# Append to original DataFrame
df_olca = pd.concat([df_olca, new_df], ignore_index=True)


#%% Add values shared by both inputs and ref flow

df_olca['ProcessCategory'] = '48-49: Transportation and Warehousing'
df_olca['Context'] = 'Technosphere Flows / 48-49: Transportation and Warehousing'
df_olca['FlowType'] = 'PRODUCT_FLOW'
df_olca['avoided_product'] = False
df_olca['location'] = 'US'
df_olca['Year'] = 2017


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
    extract_sources_from_process_meta, extract_dqsystems

(process_meta, source_objs) = extract_sources_from_process_meta(
    process_meta, bib_path = working_dir / 'transport_sources.bib')

(process_meta, actor_objs) = extract_actors_from_process_meta(process_meta)

dq_objs = extract_dqsystems(meta['DQI']['dqSystem'])

process_meta['dq_entry'] = format_dqi_score(meta['DQI']['Process'])

# generate dictionary of location objects
location_objs = build_location_dict(df_olca, locations)


#%% Create json file
from flcac_utils.generate_processes import build_flow_dict, \
    build_process_dict, write_objects, validate_exchange_data
from flcac_utils.util import assign_year_to_meta

validate_exchange_data(df_olca)
flows, new_flows = build_flow_dict(df_olca)
processes = {}
for year in df_olca.Year.unique():
    ### *** I dont think this is relevant since we have 1 year of data
    process_meta = assign_year_to_meta(process_meta, year)
    # Update time period to match year for each region

    p_dict = build_process_dict(df_olca.query('Year == @year'),
                                flows,
                                meta=process_meta,
                                loc_objs=location_objs,
                                source_objs=source_objs,
                                actor_objs=actor_objs,
                                dq_objs=dq_objs,
                                )
    processes.update(p_dict)

write_objects('uslci-transport', flows, new_flows, processes,
              location_objs, source_objs, actor_objs, dq_objs,
              )
'''
write_objects('uslic-transport', flows, new_flows, processes,
              location_objs, source_objs, actor_objs, dq_objs,
              )
'''












