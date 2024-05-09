import yaml
import numpy as np
import db_root as db
import pandas as pd

# Create a Biomet.CSV file formatted for EddyPro

with open('config_files/config.yml') as yml:
    config = yaml.safe_load(yml)
with open('config_files/biomet_csv_from_binary_database.yml') as yml:
    config.update(yaml.safe_load(yml))
config.update(db.config)

def make(siteID,dateRange):
    Range_index = pd.DatetimeIndex(dateRange)
    traces={}
    columns = []
    df = pd.DataFrame()
    Years = range(Range_index.year.min(),Range_index.year.max()+1)
    root = config['RootDirs']['Database']
    file = f"{siteID}/{config['database_info']['stage']}/{config['database_info']['timestamp']['name']}"
    tv = [np.fromfile(f"{root}{YYYY}/{file}",config['database_info']['timestamp']['dtype']) for YYYY in Years]
    tv = np.concatenate(tv,axis=0)
    DT = pd.to_datetime(tv-config['database_info']['timestamp']['base'],unit=config['database_info']['timestamp']['base_unit']).round('S')
    traces['timestamp'] = DT.floor('Min').strftime(config['timestamp']['fmt'])
    columns.append((config['timestamp']['output_name'],config['timestamp']['units']))
    for trace_name,trace_info in config['traces'].items():
        try:
            file = f"{siteID}/{config['database_info']['stage']}/{trace_name}"
            trace = [np.fromfile(f"{root}{YYYY}/{file}",config['database_info']['traces']['dtype']) for YYYY in Years]
            traces[trace_info['output_name']]=np.concatenate(trace,axis=0)
        except:
            traces[trace_info['output_name']]=np.empty(tv.shape)*np.nan
        columns.append((trace_info['output_name'],trace_info['units']))
    df = pd.DataFrame(data=traces,index=DT)
    df = df.loc[((df.index>=Range_index.min())&(df.index<= Range_index.max()))]

    df.columns = pd.MultiIndex.from_tuples(columns)
    df = df.fillna(config['na_value'])
    print(config)
    df.to_csv(f"{config['RootDirs']['Highfreq']}/{siteID}/{config['filenames']['biomet_file']}")