# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import re
import csv
import sys
import shutil
import zipfile
import datetime
import traceback
import importlib
import numpy as np
import pandas as pd
import configparser
from pathlib import Path
from io import TextIOWrapper
import readLiConfigFiles as rLCF
import xml.etree.ElementTree as ET
from HelperFunctions import EventLog as eL
importlib.reload(rLCF)

# Copy ghg or dat files and shift timestamp in file name if needed
# useful to get data from sever for local run, or to copy from a datadump folder to more centralized repo
# Called from preProcessing module, defined here to allow copying to be done in parallel
# Compares against existing data to avoid re-copying files
# if dateRange provided, will limit to files within the range
def copy_and_check_files(inName,in_dir,out_dir,fileInfo,byYear=True,byMonth=True,checkList=[],dateRange=None):
    # return empty list if 
    empty = [None,None,None,None]
    if inName.endswith(fileInfo['extension']) and fileInfo['searchTag'] in inName and inName not in checkList:
        srch = re.search(fileInfo['search'], inName.rsplit('.',1)[0]).group(0)
        if srch is not None:
            file_prototype = inName.replace(srch,fileInfo['ep_date_pattern'])
            TIMESTAMP =  datetime.datetime.strptime(srch,fileInfo['format'])
            if fileInfo['timeShift'] is not None:
                TIMESTAMP = TIMESTAMP+datetime.timedelta(minutes=fileInfo['timeShift'])
                timeString = datetime.datetime.strftime(TIMESTAMP,fileInfo['format'])
                outName = inName.replace(srch,timeString)
            else:
                outName=inName
            
            if byYear==True:
                out_dir = f'{out_dir}/{str(TIMESTAMP.year)}/'
                if byMonth==True:
                    out_dir = f'{out_dir}{str(TIMESTAMP.month).zfill(2)}/'
            
            if dateRange is None or (TIMESTAMP >= dateRange.min() and TIMESTAMP <= dateRange.max()):
                if os.path.isfile(f'{out_dir}/{outName}')==False:
                    os.makedirs(out_dir, exist_ok=True)
                    shutil.copy2(f"{in_dir}/{inName}",f"{out_dir}/{outName}")
                return([TIMESTAMP,f"{in_dir}/{inName}",outName,file_prototype])
            else:return(empty)
        else:return(empty)
    # return the empty list if any condition failed
    return(empty)
    
class Parser():
    def __init__(self,config,metaDataTemplate=None):
        self.config = config
        # Define statistics to aggregate raw data by, see configuration
        self.agg = [key for key, value in self.config['monitoringInstructions']['dataAggregation'].items() if value is True]
        if metaDataTemplate is not None:
            self.metaDataTemplate,self.fileDescription = self.readMetaData(open(metaDataTemplate))
            self.fileDescription.update(self.config['dat'])

    def readFile(self,file):
        timestamp=file[0]
        filepath=file[1]
        if filepath.endswith('.ghg'):
            d_agg,metaData = self.extractGHG(filepath,timestamp)
        else:
            d_agg, d_names = self.readData(filepath,self.fileDescription,timestamp)
            metaData = self.metaDataTemplate.copy()
            metaData.update(d_names)
        
        metaData = pd.DataFrame(metaData,index=[timestamp])
        metaData.index.name = 'TIMESTAMP'
        return(d_agg,metaData)

    def extractGHG(self,filepath,timestamp):
        base = os.path.basename(filepath).rstrip('.ghg')
        ghgInventory = {}
        with zipfile.ZipFile(filepath, 'r') as ghgZip:
            subFiles=ghgZip.namelist()
            # Get all possible contents of ghg file
            # For now only concerned with .data and .metadata
            # Will expand to biomet and config/calibration files later
            for f in subFiles:
                ghgInventory[f.replace(base,'')]=f
            metaData,fileDescription = self.readMetaData(TextIOWrapper(ghgZip.open(ghgInventory['.metadata']), 'utf-8'))
            fileDescription.update(self.config['ghg'])
            if hasattr(fileDescription, 'skip_rows') == False:
                fileDescription['skip_rows'] = int(fileDescription['header_rows'])-1
                fileDescription['header_rows'] = [0]

            d_agg, d_names = self.readData(ghgZip.open(ghgInventory['.data']),fileDescription,timestamp)
            metaData.update(d_names)
        return(d_agg,metaData)

    def readMetaData(self,metaDataFile):
        # Parse the .metadata file included in the .ghg file
        # Or parse a userdefined template
        # Extract file description to parse data
        # Dump relevant metaData values to dataframe for tracking
        metaData = configparser.ConfigParser()
        metaData.read_file(metaDataFile)
        metaData = {key:dict(metaData[key]) for key in metaData.keys()}

        # Isolate and parse file description for reading data files
        fileDescription = metaData['FileDescription'].copy()
        fileDescription['delimiter'] = self.config['delimiters'][fileDescription['separator']].encode('ascii','ignore').decode('unicode_escape')

        # Reformat for dumping to DataFrame        
        metaData = {(k1,k2):val for k1 in metaData.keys() for k2,val in metaData[k1].items()}
        return(metaData,fileDescription)
    
    def readData(self,dataFile,fileDescription,timestamp):
        # read the raw high frequency data
        # parse the column names and output desired aggregation statistics for each raw data file
        if 'na_values' in fileDescription.keys():
            data = pd.read_csv(dataFile,skiprows=fileDescription['skip_rows'],header=fileDescription['header_rows'],sep=fileDescription['delimiter'],na_values=fileDescription['na_values'])
        else:
            data = pd.read_csv(dataFile,skiprows=fileDescription['skip_rows'],header=fileDescription['header_rows'],sep=fileDescription['delimiter'])
            
        if fileDescription['data_label'] != 'Not set':
            # .ghg data files contain a "DATA" label if first column which isn't needed
            data = data.drop(data.columns[0],axis=1)
        # Parse units from metadata if not included in headers
        if len(fileDescription['header_rows']) == 1:
            unit_list = [value for key,value in fileDescription.items() if 'unit_in' in key]
            data.columns = [data.columns,unit_list]

        # generate dict of column names to add back into Metadata
        col_names = {}
        for i,c in enumerate(data.columns.get_level_values(0)):
            col_names[('Custom',f'col_{i+1}_header_name')] = c

        # Generate the aggregation statistics, but only for numeric columns
        data = data._get_numeric_data()
        d_agg = data.agg(self.agg)
        d_agg['Timestamp'] = timestamp
        d_agg.set_index('Timestamp', append=True, inplace=True)
        d_agg = d_agg.reorder_levels(['Timestamp',None]).unstack()
        return(d_agg,col_names)
