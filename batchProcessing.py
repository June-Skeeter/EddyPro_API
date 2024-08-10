# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import re
import sys
import shutil
import zipfile
import datetime
import importlib
import subprocess
import numpy as np
import pandas as pd
import configparser
from io import TextIOWrapper
import readLiConfigFiles as rLCF
# import xml.etree.ElementTree as ET
# from HelperFunctions import EventLog as eL
importlib.reload(rLCF)


def pasteWithSubprocess(source, dest, option = 'copy'):    
    cmd=None
    if sys.platform.startswith("darwin"): 
        if option == 'copy':
            cmd=['cp', source, dest]
        elif option == 'move':
            cmd=['mv',source,dest]
    elif sys.platform.startswith("win"): 
        cmd=[option, source, dest]
    if cmd:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print(proc)
# Copy ghg or dat files and shift timestamp in file name if needed
# useful to get data from sever for local run, or to copy from a datadump folder to more centralized repo
# Called from preProcessing module, defined here to allow copying to be done in parallel
# Compares against existing data to avoid re-copying files
# if dateRange provided, will limit to files within the range

def copy_and_check_files(inName,in_dir,out_dir,fileInfo,byYear=True,byMonth=True,checkList=[],dateRange=None):
    # return empty list if 
    empty = [None,None,None,None]
    if inName.endswith(fileInfo['extension']) and fileInfo['searchTag'] in inName and inName not in checkList and fileInfo['excludeTag']+'_'+inName not in checkList:
        srch = re.search(fileInfo['search'], inName.rsplit('.',1)[0]).group(0)
        if srch is not None:
            file_prototype = inName.replace(srch,fileInfo['ep_date_pattern'])
            TIMESTAMP =  datetime.datetime.strptime(srch,fileInfo['format'])
            if fileInfo['timeShift'] != 'None':
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
                source = os.path.abspath(f"{in_dir}/{inName}")
                dest = os.path.abspath(f"{out_dir}/{outName}")
                if os.path.isfile(dest)==False:
                    os.makedirs(out_dir, exist_ok=True)
                    pasteWithSubprocess(source, dest)
                return([TIMESTAMP,source,outName,file_prototype])
            else:return(empty)
        else:return(empty)
    # return the empty list if any condition failed
    return(empty)

class Parser():
    def __init__(self,config,metaDataTemplate='None'):
        self.config = config
        # Define statistics to aggregate raw data by, see configuration
        self.agg = [key for key, value in self.config['monitoringInstructions']['dataAggregation'].items() if value is True]
        if metaDataTemplate != 'None':
            self.metaDataTemplate,self.fileDescription = self.readMetaData(open(metaDataTemplate))
            self.fileDescription.update(self.config['dat'])

    def readFile(self,file):
        timestamp=file[0]
        filepath=file[1]
        if filepath.endswith('.ghg'):
            try:
                d_agg,metaData = self.extractGHG(filepath,timestamp)
            except:
                print(f"extraction failed for: {filepath}")
                d_agg,metaData=None,None
                pass
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


        data = data.dropna(how='all',axis=1)
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

class runEddyPro():
    def __init__(self,epRoot,subsetNames=['1'],priority = 'normal',debug=False):
        self.epRoot = os.path.abspath(epRoot)
        self.priority = priority
        self.debug = debug
        self.tempDir = {}
        self.subsetNames = subsetNames
        for subsetName in subsetNames:
            self.tempDir[f"{subsetName}"] = os.path.abspath(f"{os.getcwd()}/temp/{subsetName}/")
            if self.debug == False and os.path.isdir(self.tempDir[f"{subsetName}"]):
                shutil.rmtree(self.tempDir[f"{subsetName}"])
            os.makedirs(self.tempDir[f"{subsetName}"],exist_ok=True)

    def setUp(self,toRun):
        fname = os.path.basename(toRun)
        toRun = os.path.abspath(toRun)
        cwd = os.getcwd()
        pid = os.getpid()
        subsetName = [s for s in self.subsetNames if s in toRun][0]
        batchRoot = os.path.abspath(f'{self.tempDir[subsetName]}/{pid}')
        try:
            shutil.rmtree(batchRoot)
        except:
            pass
        bin = os.path.abspath(f'{batchRoot}/bin/')
        os.makedirs(bin)        
        pasteWithSubprocess(self.epRoot, bin)
        ini = os.path.abspath(f'{batchRoot}/ini/')
        os.makedirs(ini)
        processing = os.path.abspath(f'{ini}/processing.eddypro')
        pasteWithSubprocess(toRun,processing,option='move')
        return(bin)

    def rpRun(self,toRun):
        bin = self.setUp(toRun)
        runEddyPro_rp=os.path.abspath(f'{bin}/runEddyPro_rp.bat')
        with open(runEddyPro_rp, 'w') as batch:
            contents = f'cd {bin}'
            P = self.priority.lower().replace(' ','')
            contents+='\nSTART powershell  ".\\eddypro_rp.exe | tee rp_processing_log.txt"'
            contents+='\nping 127.0.0.1 -n 6 > nul'
            contents+=f'\nwmic process where name="eddypro_rp.exe" CALL setpriority "{self.priority}"'
            contents+='\nping 127.0.0.1 -n 6 > nul'
            contents+='\nEXIT'
            batch.write(contents)

        subprocess.run(['cmd', '/c', runEddyPro_rp], capture_output=True)

        pasteWithSubprocess(
            os.path.abspath(f'{bin}/rp_processing_log.txt'),
            os.path.abspath(toRun.replace('.eddypro','_log.txt'))
        )
        return(os.path.split(bin)[0])

    def fccRun(self,toRun):
        bin = self.setUp(toRun)
        runEddyPro_fcc=os.path.abspath(f'{bin}/runEddyPro_fcc.bat')
        with open(runEddyPro_fcc, 'w') as batch:
            contents = f'cd {bin}'
            P = self.priority.lower().replace(' ','')
            contents+='\nSTART powershell  ".\\eddypro_fcc.exe | tee fcc_processing_log.txt"'
            contents+='\nping 127.0.0.1 -n 6 > nul'
            contents+=f'\nwmic process where name="eddypro_fcc.exe" CALL setpriority "{self.priority}"'
            contents+='\nping 127.0.0.1 -n 6 > nul'
            contents+='\nEXIT'
            batch.write(contents)

        subprocess.run(['cmd', '/c', runEddyPro_fcc], capture_output=True)

        pasteWithSubprocess(
            os.path.abspath(f'{bin}/fcc_processing_log.txt'),
            os.path.abspath(toRun.replace('.eddypro','_log.txt'))
        )
        return(os.path.split(bin)[0])
