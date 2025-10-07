#%% PURPOSE ##

'''

The purpose of this script is to build olca process objects that will contain
the 2017 CFS PUF derived transport data for commodities defined according to
the 2017 Standard Classification of Transported Goods (SCTG) Codes.

The distances associated with each transport mode within a commodity category
are weighted by the total moass of that commodity transported via the
respective transportation mode.

'''


#%% SETUP ##

## DEPENDENCIES ##
import pandas as pd
import numpy as np
from pathlib import Path
import yaml
from esupy.util import make_uuid
import os

# *** add code to read in the yaml files for filling in metadata

# Read in CSV file created by 'commodity transport distances.py'
csv_path = r'C:\Users\mchristie\Code\uslci-transport\Weighted Commodity Transport Distances.csv'
df_olca = pd.read_csv(csv_path)
df_olca = df_olca.drop(columns=['Mass Shipped (kg)', 'Avg. Dist. Shipped (km)', 'Mass Frac. by Mode'])

# Create empty df_olca that includes all schema requirements
# For ref. flow in each process, create new flow?
schema = ['ProcessID', # Where is this assigned
          'ProcessCategory', # '48-49: Transportation and Warehousing / *** add a new category?? ***'
          'ProcessName', # Commodity specific process names to be developed
          'FlowUUID', # Use existing transport flow UUIDs in USLCI 
          'FlowName', # Use existing transport flow names in USLCI
          'Context', # Use mode specific flow category from transportation and warehousing
          'IsInput', # true for flows receiving data from csv | isInput = false for single refFlow per process
          'FlowType', # 'PRODUCT_FLOW'
          'reference', 
          'default_provider', # Define by transport mode in metadata file
          'description', # Define within process metadata file
          'amount', # Input values from CSV | ref. flow is 1 kg of commodity
          'unit', # Input values = kg*km | ref. flow = kg
          'avoided_product', # false
          'exchange_dqi', # define in meta data file
          'location'] # US

# Add schema columns to df_olca
for column in schema:
    df_olca[column] = ''
    
# Move values from 'Weighted Dist. Shipped (km)' to 'amount'
# Remove 'Weighted Dist. Shipped (km)' column
df_olca['amount'] = df_olca['Weighted Dist. Shipped (km)']
df_olca.drop('Weighted Dist. Shipped (km)', axis=1, inplace=True)

# Add values that are the same for all inputs
### Reference flows have not been added yet
df_olca['IsInput'] = True
df_olca['reference'] = False
df_olca['unit'] = 'kg*km'

# Add reference flows 

