# Find the database root path
# Written by June Skeeter March 2024

import os
import sys
import yaml

config_fn = '_config.yml'

def get_config(fn='_config.yml'):
    with open(fn) as f:
        config = yaml.safe_load(f)
    return(config)


# 1 Search for _config.yml in root of Project Folder
if os.path.isfile(config_fn):
    db_root = get_config(config_fn)

# 2 Search environment variables for UBC_PC_Setup
# Repeat 1 & 2, prompt for input as last resort
else:
    pth = [v for v in os.environ.values() if 'UBC_PC_Setup' in v]
    if len(pth)>0:
        pth = pth[0]+'/'
        if os.path.isfile(pth+config_fn):
            config = get_config(pth+config_fn)
# try:
#     db_root = config['Database']['root']
#     db_ini = db_root+'Calculation_Procedures/TraceAnalysis_ini/'
#     db_hf = config['Highfreq']['root']
# except:
#     print('Not root configurations found')