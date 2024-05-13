# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import re
import csv
import sys
from pathlib import Path
import zipfile
import configparser
import numpy as np
import pandas as pd
from pathlib import Path
from io import TextIOWrapper
import xml.etree.ElementTree as ET
import datetime
import importlib
import traceback
import shutil

import readLiConfigFiles as rLCF
importlib.reload(rLCF)
from HelperFunctions import EventLog as eL

# Copy ghg or dat files and shift timestamp in file name if needed
# useful to get data from sever for local run, or to copy from a datadump folder to more centralized repo
# Called from preProcessing module, defined here to allow copying to be done in parallel
def copy_and_check_files(filename,in_dir,out_dir,fileInfo,byYear=True,byMonth=True):
    if filename.endswith(fileInfo['extension']) and fileInfo['searchTag'] in filename:
        srch = re.search(fileInfo['search'], filename.rsplit('.',1)[0]).group(0)
        if srch is not None:
            name_pattern = filename.replace(srch,fileInfo['ep_date_pattern'])
            TIMESTAMP =  datetime.datetime.strptime(srch,fileInfo['format'])
            if fileInfo['timeShift'] is not None:
                TIMESTAMP = TIMESTAMP+datetime.timedelta(minutes=fileInfo['timeShift'])
                timeString = datetime.datetime.strftime(TIMESTAMP,fileInfo['format'])
                outName = filename.replace(srch,timeString)
            else:
                outName=filename
            if byYear==True:
                out_dir = f'{out_dir}/{str(TIMESTAMP.year)}/'
                if byMonth==True:
                    out_dir = f'{out_dir}{str(TIMESTAMP.month).zfill(2)}/'
            if os.path.isfile(f'{out_dir}/{outName}')==False:
                os.makedirs(out_dir, exist_ok=True)
                shutil.copy2(f"{in_dir}/{filename}",f"{out_dir}/{outName}")
            return([TIMESTAMP,filename,name_pattern])
        else: return([None,None,None])
    else: 
        return([None,None,None])
    
class Parser():
    def __init__(self,config,metaDataTemplate=None):
        self.config = config
        self.agg = [key for key, value in self.config['aggregation'].items() if value is True]
        if metaDataTemplate is not None:
            self.metaDataTemplate,self.fileDescription = self.readMetaData(open(metaDataTemplate))
            self.fileDescription.update(self.config['dat'])


    def readFile(self,file):
        timestamp=file[0]
        filepath=file[1]
        if filepath.endswith('.ghg'):
            d_agg,metaData = self.extractGHG(filepath,timestamp)
        else:
            d_agg = self.readData(filepath,self.fileDescription,timestamp)
            metaData = self.metaDataTemplate.copy()
        del metaData['DEFAULT']
        mdDF = {(k1,k2):val for k1 in metaData.keys() for k2,val in metaData[k1].items()}
        mdDF = pd.DataFrame(mdDF,index=[timestamp])
        mdDF.index.name = 'TIMESTAMP'
        return(d_agg,mdDF)

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

            d_agg = self.readData(ghgZip.open(ghgInventory['.data']),fileDescription,timestamp)
        return(d_agg,metaData)

    def readMetaData(self,metaDataFile):
        # Parse the .metadata file included in the .ghg file
        # Or parse a userdefined template
        metaData = configparser.ConfigParser()
        metaData.read_file(metaDataFile)
        metaData = {key:dict(metaData[key]) for key in metaData.keys()}

        # Isolate and parse file description for reading data files
        fileDescription = metaData['FileDescription'].copy()
        fileDescription['delimiter'] = self.config['delimiters'][fileDescription['separator']].encode('ascii','ignore').decode('unicode_escape')
        
        return(metaData,fileDescription)
    
    def readData(self,dataFile,fileDescription,timestamp):
        data = pd.read_csv(dataFile,skiprows=fileDescription['skip_rows'],header=fileDescription['header_rows'],sep=fileDescription['delimiter'])
        if fileDescription['data_label'] != 'Not set':
            # .ghg data files contain a "DATA" label if first column which isn't needed
            data = data.drop(data.columns[0],axis=1)

        # Parse units from metadata if not included in headers
        if len(fileDescription['header_rows']) == 1:
            unit_list = [value for key,value in fileDescription.items() if 'unit_in' in key]
            data.columns = [data.columns,unit_list]
        data = data._get_numeric_data()
        d_agg = data.agg(self.agg)
        d_agg['Timestamp'] = timestamp
        d_agg.set_index('Timestamp', append=True, inplace=True)
        d_agg = d_agg.reorder_levels(['Timestamp',None]).unstack()
        # print(d_agg)
        # d_agg = d_agg.to_dict()#orient='tight')
        # print(d_agg)
        return(d_agg)

        
# def get_delimiter(file_path, bytes = 4096):
#     # Autodetect file delimiter
#     sniffer = csv.Sniffer()
#     data = open(file_path, "r").read(bytes)
#     delimiter = sniffer.sniff(data).delimiter
#     return (delimiter)