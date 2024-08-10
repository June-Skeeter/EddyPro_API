# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import re
import sys
import yaml
import time
import json
import shutil
import fnmatch
import argparse
from glob import glob
import batchProcessing
import importlib
import numpy as np
import pandas as pd
import configparser
from pathlib import Path
from functools import partial
from collections import Counter, defaultdict
from multiprocessing import Pool
from datetime import datetime,date
from HelperFunctions import progressbar, queryBiometDatabase,dumpToBiometDatabase
importlib.reload(batchProcessing)

# Directory of current script
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
# Set to cwd to location of the current script
os.chdir(dname)

# Default arguments
defaultArgs = {
    'siteID':'SomeSite',
    'sourceDir':'',
    'dateRange':[date(datetime.now().year,1,1).strftime("%Y-%m-%d"),datetime.now().strftime("%Y-%m-%d")],
    'fileType':'ghg',
    'eddyProStaticConfig':'ini_files/LabStandard_Advanced.eddypro',
    'eddyProDynamicConfig':'ini_files/eddyProDynamicConfig.eddypro',
    'GHG_Metadata_Template':'ini_files/GHG_Metadata_Template.metadata',
    'metaDataTemplate':'None',
    'processes':os.cpu_count()-2,
    'priority':'normal',
    'debug':False,
    'testSet':0,
    'reset':False,
    'name':'TestRun',
    'biometData':'None',
    'dynamicMetadata':'None',
    'userDefinedEddyProSettings':{},
    'priority':'High Priority',
    'searchTag':'',
    'timeShift':'None',
    'biometUser':False,
    'metaDataUpdates':'None',
    }

class eddyProAPI():
    def __init__(self,**kwargs):
        os.chdir(dname)
        if isinstance(kwargs, dict):
            pass
        elif os.path.isdir(kwargs):
            with open(kwargs) as yml:
                kwargs = yaml.safe_load(yml)
        else:
            sys.exit(f"Provide a valid set of arguments, either from a yaml file or a dict")

        # Apply defaults where not defined
        kwargs = defaultArgs | kwargs
        # add arguments as class attributes
        for k, v in kwargs.items():
            setattr(self, k, v)

        # Turn of multiprocessing when debugging and limit number of files
        if self.debug == True:
            self.processes = 1
        
        self.dateRange = pd.DatetimeIndex(self.dateRange)
        
        start_str = self.dateRange[0].strftime('%Y%m%d%H%M')
        end_str = self.dateRange[-1].strftime('%Y%m%d%H%M')
        self.project_id = f'{self.name}_{start_str}_{end_str}'

        # LICOR uses a modifiation of .ini format (.eddypro) to define EddyPro runs
        eddyProStaticConfig = self.eddyProStaticConfig
        self.eddyProStaticConfig = configparser.ConfigParser()
        self.eddyProStaticConfig.read(eddyProStaticConfig)

        # Dynamic updates as as a separate configuration to be processed using eval statements
        eddyProDynamicConfig = self.eddyProDynamicConfig
        self.eddyProDynamicConfig = configparser.ConfigParser()
        self.eddyProDynamicConfig.read(eddyProDynamicConfig)

        # Template file for dumping group metadata values
        GHG_Metadata_Template = self.GHG_Metadata_Template
        self.GHG_Metadata_Template = configparser.ConfigParser()
        self.GHG_Metadata_Template.read(GHG_Metadata_Template)

        # Read yaml configurations        
        with open('config_files/config.yml') as yml:
            self.config = yaml.safe_load(yml)
        self.config['siteID'] = self.siteID
        groupID = '\d+'
        self.genericID = eval(self.config['stringTags']['groupID'])
        # Exit if user paths are not provided
        if os.path.isfile('config_files/user_path_definitions.yml'):
            with open('config_files/user_path_definitions.yml') as yml:
                self.config.update(yaml.safe_load(yml))
        else:
            sys.exit(f"Missing {'config_files/user_path_definitions.yml'}")

        # Setup paths using definitions from config file
        self.config['Paths'] = {}
        for key,val in self.config['RelativePaths'].items():
            self.config['Paths'][key] = eval(val)
            if os.path.isdir(self.config['Paths'][key]) == False:
                os.makedirs(self.config['Paths'][key])

        for key in self.config['metadataFiles'].keys():
            self.config[key] = self.config['Paths']['meta_dir']+key+'.csv'
            self.config['metadataFiles'][key]['filepath_or_buffer'] = self.config[key]

        # Read the existing metadata from a previous run if they exist
        if self.reset == True: self.resetInventory()

        # Create the directories if they doesn't exist
        os.makedirs(self.config['Paths']['meta_dir'],exist_ok=True)
        os.makedirs(self.config['Paths']['raw'],exist_ok=True)

        # On the fly Biomet and dynamicMetadata csv file generation
        # For Biomet.net users only
        if self.biometUser and os.path.isdir(self.config['BiometUser']['Biomet.net']):
            auxilaryDpaths=queryBiometDatabase(
                siteID=self.siteID,
                outputPath = self.config['Paths']['meta_dir'],
                biometPath = self.config['BiometUser']['Biomet.net'],
                database = self.config['BiometUser']['Database'],
                dateRange = self.dateRange)
            for key,value in auxilaryDpaths.items():
                setattr(self, key, value)

        if self.biometData != 'None':
            self.biometDataTable = pd.read_csv(self.biometData)
        
        # Read the existing metadata from a previous run if they exist
        read_files = {key:value for key,value in self.config['metadataFiles'].items() if value['filepath_or_buffer'].startswith('f"')==False}
        if sum([os.path.isfile(value['filepath_or_buffer']) for value in read_files.values()]) == len(read_files):
            # Some metadata need their dtypes explicitly specified when reading .csv files all others treated as "objects"
            dtypes = defaultdict(lambda:'object',groupID='int')
            for category, dtype in {'groupBy':'string','track':'float','pass':'string'}.items():
                for key,value in self.config['monitoringInstructions']['metaData'][category].items():
                    for val in value:
                        dtypes[(key,val)]=dtype
            for key,value in read_files.items():
                print(key,value)
                setattr(self, key,(pd.read_csv(dtype=dtypes,**value)))
        else:
            for key in read_files.keys():
                setattr(self, key,pd.DataFrame())            

    def resetInventory(self):
        RESET = input(f"WARNING!! You are about to complete a reset: type RESET to continue, provide any other input + enter to exit the application")
        if RESET.upper() == 'RESET':
            if os.path.isdir(self.config['Paths']['meta_dir']):
                print(f"Deleting contents of : {self.config['Paths']['meta_dir']}")
                shutil.rmtree(self.config['Paths']['meta_dir'])
            if os.path.isdir(self.config['Paths']['raw']):
                print(f"Deleting contents of : {self.config['Paths']['raw']}")
                shutil.rmtree(self.config['Paths']['raw'])
        else:
            sys.exit('Quitting')
        # # Re-create the directories if they doesn't exist
        # os.makedirs(self.config['Paths']['meta_dir'],exist_ok=True)
        # os.makedirs(self.config['Paths']['raw'],exist_ok=True)

class preProcessing(eddyProAPI):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
                
        # Initiate parser class, defined externally to facilitate parallel processing
        self.Parser = batchProcessing.Parser(self.config,self.metaDataTemplate)
        if self.debug == False:
            self.searchRawDir()
            self.readFiles()
            self.groupAndFilter()
        
    def searchRawDir(self):
        # Build the file inventory of the "raw" directory and copy new files if needed
        # Option to shift the timestamp: copy data and rename using shifted timestamp
        # timeShift will only be applied to data copied from another directory
        search_dirs = [self.config['Paths']['raw']]
        shiftTime = ['None']
        if os.path.isdir(self.sourceDir):
            search_dirs.append(self.sourceDir)
            shiftTime.append(self.timeShift)

        T1 = time.time()
        fileInfo = self.config[self.fileType]
        fileInfo['searchTag'] = self.searchTag
        fileInfo['excludeTag'] = self.genericID

        # Walk the search directories
        for search,timeShift in zip(search_dirs,shiftTime):
            fileInfo['timeShift'] = timeShift
            for dir, _, fileList in os.walk(search):
                # Exclude files that have already been processed from fileList
                if 'source' in self.fileInventory.columns:
                    source_names = [os.path.basename(f) for f in self.fileInventory['source'].values]
                else:
                    source_names = []
                fileList = [f for f in fileList if f not in source_names]
                if len(fileList)>0:
                    print(f'Searching {dir}')
                    dout = []
                    if (__name__ == 'eddyProAPI' or __name__ == '__main__') and self.processes>1:
                        # run routine in parallel
                        pb = progressbar(len(fileList),'')
                        with Pool(processes=self.processes) as pool:
                            max_chunksize=10
                            chunksize=min(int(np.ceil(len(fileList)/self.processes)),max_chunksize)
                            for out in pool.imap(partial(batchProcessing.copy_and_check_files,in_dir=dir,out_dir=self.config['Paths']['raw'],fileInfo=fileInfo,dateRange=self.dateRange),fileList,chunksize=chunksize):
                                pb.step()
                                dout.append(out)
                            pool.close()
                            pb.close()
                    else:
                        # run routine sequentially for debugging
                        testOffset=0
                        for i,filename in enumerate(fileList):
                            if self.debug == False or i < self.testSet + testOffset or self.testSet == 0:
                                out = batchProcessing.copy_and_check_files(filename,dir,self.config['Paths']['raw'],fileInfo=fileInfo,dateRange=self.dateRange)
                                if out[0] is None and self.testSet > 0:
                                    testOffset += 1
                                dout.append(out)
                            # else:
                            #     print(filename,dir)
                    # Dump results to inventory
                    # source and filename will be different if a timeShift is applied when copying
                    df = pd.DataFrame(columns=['TIMESTAMP','source','filename','file_prototype'],data=dout)
                    # Add empty columns for auxillary information
                    df[['Filter Flags']]=self.config['stringTags']['NaN']
                    df['TIMESTAMP'] = pd.DatetimeIndex(df['TIMESTAMP'])
                    df = df.set_index('TIMESTAMP')
                    # drop rows where filename_final are missing
                    df = df.loc[df['filename'].isnull()==False]
                    # Merge with existing inventory   
                    self.fileInventory = pd.concat([self.fileInventory,df])
        
        # Quit if no data were found
        if self.fileInventory.empty:
            sys.exit('No Data Found')
        # Resample to get timestamp on consistent half-hourly intervals
        self.fileInventory = self.fileInventory.resample('30T').first()
        # Fill empty string columns
        self.fileInventory = self.fileInventory.fillna(self.config['stringTags']['NaN'])

        # Sort so that oldest files get processed first
        self.fileInventory = self.fileInventory.sort_index()#ascending=False)
        # Save inventory
        self.fileInventory.to_csv(self.config['fileInventory'])
        print('Files Search Complete, time elapsed: ',time.time()-T1)
    
    def readFiles(self):
        T1 = time.time()
        print('Reading Data')
        # Parse down to just files that need to be processed (those which have not already been assigned a group or set to exclude)
        to_process = self.fileInventory.loc[(
            (self.fileInventory['filename'].str.endswith(self.fileType))&
            (self.fileInventory['filename'].str.startswith(self.config['stringTags']['exclude'])==False)&
            (self.fileInventory['filename'].str.match(self.genericID)==False)&
            (self.fileInventory.index>=self.dateRange.min())&(self.fileInventory.index<=self.dateRange.max())
            )].copy()

        # Call file handler to parse files in parallel (default) or sequentially for troubleshooting
        pathList=(self.config['Paths']['raw']+to_process.index.strftime('%Y/%m/')+to_process['filename'])
        if (__name__ == 'eddyProAPI' or __name__ == '__main__') and self.processes>1 and len(pathList) > 0:
            # run routine in parallel
            pb = progressbar(len(pathList),'')
            with Pool(processes=self.processes) as pool:
                max_chunksize=4
                chunksize=min(int(np.ceil(len(pathList)/self.processes)),max_chunksize)
                for out in pool.imap(partial(self.Parser.readFile),pathList.items(),chunksize=chunksize):
                    pb.step()
                    if out[0] is not None:
                        self.rawDataStatistics = pd.concat([self.rawDataStatistics,out[0]])
                        self.metaDataValues = pd.concat([self.metaDataValues,out[1]])
                pool.close()
                pb.close()
        else:
            # run routine sequentially
            for i, (timestamp,file) in enumerate(pathList.items()):
                if i < self.testSet or self.testSet == 0:
                    T2 = time.time()
                    out = self.Parser.readFile((timestamp,file))
                    if out[0] is not None:
                        self.rawDataStatistics = pd.concat([self.rawDataStatistics,out[0]])
                        self.metaDataValues = pd.concat([self.metaDataValues,out[1]])
                        print(f'{file} complete, time elapsed:git ',time.time()-T2)
        
        self.rawDataStatistics.to_csv(self.config['rawDataStatistics'])
        self.metaDataValues.to_csv(self.config['metaDataValues'])
        print('Reading Complete, time elapsed: ',time.time()-T1)
        if self.metaDataUpdates != 'None':
            print('Applying Manual Metadata Adjustments')
            self.usermetaDataUpdates() 
            print('Manual Metadata Adjustments Complete')

    
    def usermetaDataUpdates(self):
        df = pd.read_csv(self.metaDataUpdates,header=[0,1])
        df[('TIMESTAMP','Start')] = pd.to_datetime(df[('TIMESTAMP','Start')])
        df[('TIMESTAMP','End')] = pd.to_datetime(df[('TIMESTAMP','End')])
        df[('TIMESTAMP','End')] = df[('TIMESTAMP','End')].fillna(self.metaDataValues.index.max())
        for i,row in df.iterrows():
            for col in row.index:
                if col[0] !=  'TIMESTAMP' and pd.isnull(row[col])==False:
                    self.metaDataValues.loc[((self.metaDataValues.index>=row[('TIMESTAMP','Start')]) &
                                            (self.metaDataValues.index<=row[('TIMESTAMP','End')])),col] = str(row[col])
            

    def groupAndFilter(self):
        print('Grouping by Configuration')
        # As defined in monitoringInstructions, group timestamps by site configurations to define eddyPro Runs
        # Number of samples that should be in a file
        self.metaDataValues['Timing','expectedSamples'] = (self.metaDataValues['Timing']['acquisition_frequency'].astype(float)*60*self.metaDataValues['Timing']['file_duration'].astype(float)).values
        grouper = [(key,v) 
                   for key,value in self.config['monitoringInstructions']['metaData']['groupBy'].items() 
                   for val in value
                   for v in fnmatch.filter(self.metaDataValues[key].columns,val)] 
        tracker = [(key,v) 
                   for key,value in self.config['monitoringInstructions']['metaData']['track'].items() 
                   for val in value
                   for v in fnmatch.filter(self.metaDataValues[key].columns,val)]  
        passer = [(key,v) 
                   for key,value in self.config['monitoringInstructions']['metaData']['pass'].items() 
                   if key in self.metaDataValues.columns.get_level_values(0)
                   for val in value
                   for v in fnmatch.filter(self.metaDataValues[key].columns,val)]  
        self.metaDataValues[grouper] = self.metaDataValues[grouper].fillna(self.config['stringTags']['NaN'])
        self.metaDataValues[grouper] = self.metaDataValues[grouper].replace('',self.config['stringTags']['NaN'])
        # Generate group labels based off unique configurations of groupBy values
        self.groupID = ('group','ID')
        groupLabels = pd.DataFrame(columns=pd.MultiIndex.from_tuples([self.groupID]),
                             data=(self.metaDataValues.groupby(by=grouper).grouper.group_info[0] + 1),
                             index = self.metaDataValues.index)
        if self.groupID in self.metaDataValues.columns:
            self.metaDataValues = self.metaDataValues.drop(columns=self.groupID)    
        self.metaDataValues = pd.concat([self.metaDataValues,groupLabels],axis=1)
        # get the ID values
        self.groupIDValues = groupLabels[self.groupID].unique()
        # Get statistics for tracked values by group
        self.metaDataValues[tracker] = self.metaDataValues[tracker].astype('float')
        track_cols = [self.groupID]+tracker
        self.configurationGroups = self.metaDataValues[track_cols].groupby(by=self.groupID).agg(self.Parser.agg)
        group_cols = [self.groupID]+grouper
        self.configurationGroups = self.configurationGroups.join(self.metaDataValues[group_cols].groupby(by=self.groupID).agg(['first','count']))
        pass_cols = [self.groupID]+passer
        self.configurationGroups = self.configurationGroups.join(self.metaDataValues[pass_cols].groupby(by=self.groupID).agg(['first']))

        # add the group labels to the statistics table
        groupLabels = groupLabels.T.set_index(np.repeat('', groupLabels.shape[1]), append=True).T
        if groupLabels.columns[0] in self.rawDataStatistics:
            self.rawDataStatistics = self.rawDataStatistics.drop(columns=groupLabels.columns[0])
        self.rawDataStatistics = self.rawDataStatistics.join(groupLabels)
        groupLabels.columns=[''.join(col) for col in groupLabels.columns]
        gcol = groupLabels.columns[0]
        if gcol in self.fileInventory:
            self.fileInventory = self.fileInventory.drop(columns=gcol)
        self.fileInventory = self.fileInventory.join(groupLabels)
        self.fileInventory[gcol]=self.fileInventory[gcol].fillna(self.config['intNA']).astype(np.int32)
        # Add the file_prototype template to the configuration groups
                
        self.makeMetadataFiles()
        self.filterData()
        self.renameFiles()
        
        temp = self.fileInventory[list(groupLabels.columns)+['file_prototype']].groupby(list(groupLabels.columns)).agg(['first'])
        temp.columns = pd.MultiIndex.from_product([['Custom']]+temp.columns.levels)
        self.configurationGroups = self.configurationGroups.join(temp)
        ptype = ('Custom','file_prototype','first')
        self.configurationGroups.loc[self.configurationGroups[ptype].str.match(self.genericID)==False,
                                     ptype] = self.config['stringTags']['exclude']
        
        self.saveMetadataFiles()

    def makeMetadataFiles(self):
        # Creates two files
        #   1) A .metadata file representative of all non-dynamic (e.g., canopy height) values
        #   2) A .eddypro file representing the relevant column numbers in the .dat(a) files
          
        print('Writing Metadata Files')
        eddyProGroupDefsTemplate={'Project':{}} 
        bm = 'RawProcess_BiometMeasurements'
        if self.biometData != 'None': 
            eddyProGroupDefsTemplate[bm] = {}
            for key,value in self.config['eddyProGroupDefs'][bm].items():
                if value in self.biometDataTable.columns:
                    eddyProGroupDefsTemplate[bm][key] = list(self.biometDataTable.columns).index(value)+1
        for groupID,row in self.configurationGroups.loc[:,pd.IndexSlice[:,:,('mean','first')]].iterrows():
            row = row.fillna(self.config['stringTags']['NaN'])
            # Dump the group's metadata values to a metadata file
            # Replace NaN (or tag) with empty string
            groupMetaData = {                
                L:{i[0]:(str(v)).replace(self.config['stringTags']['NaN'],'')
                for i,v in row[L].items()} for L in 
                row.index.get_level_values(0)
                    }
            dynamicVals = {
            'Instruments': max([int(i.split('_')[1]) for i in fnmatch.filter(list(groupMetaData['Instruments'].keys()),'instr_*_model')]),
            'FileDescription':max([int(i.split('_')[1]) for i in fnmatch.filter(list(groupMetaData['FileDescription'].keys()),'col_*_variable')])
            }
            metaDataFile = {}
            for section in self.GHG_Metadata_Template.keys():
                if section not in dynamicVals:
                    dynamicVals[section] = 0
                metaDataFile[section] = {}
                orderedKeys = []
                for key,value in self.GHG_Metadata_Template[section].items():
                    if section in groupMetaData and key in groupMetaData[section]:
                        metaDataFile[section][key] = groupMetaData[section][key]
                    elif section not in groupMetaData:
                        metaDataFile[section][key] = self.GHG_Metadata_Template[section][key]
                    else:
                        orderedKeys.append(key)
                for i in range(dynamicVals[section]):
                    for key in orderedKeys:
                        nkey = key.replace('*',str(i+1))
                        if nkey in groupMetaData[section]:
                            metaDataFile[section][nkey] = groupMetaData[section][nkey]

            filename = self.config['Paths']['meta_dir']+eval(self.config['groupFiles']['groupMetaData'])
            with open(filename, 'w') as groupMetaData:
                groupMetaData.write(';GHG_METADATA\n')
                cfg = configparser.ConfigParser()
                cfg.read_dict(metaDataFile)
                cfg.write(groupMetaData,space_around_delimiters=False)
                
            # Identify the column numbers relevant to EddyPro
            eddyProGroupDefs=eddyProGroupDefsTemplate.copy()
            row.index = row.index.get_level_values(1)
            variable = fnmatch.filter(row.index,'col_*_variable')
            measurement_types = fnmatch.filter(row.index,'col_*_measure_type')
            for key,value in self.config['eddyProGroupDefs']['Project'].items():
                if ('measure_type' in value.keys()) ==False:
                    value['measure_type'] = [self.config['stringTags']['NaN']]
                var_ix = [i.split('_')[1] for i,v in (row[variable]==value['variable']).items() if v == True]
                for m in value['measure_type']:
                    meas_ix = [i.split('_')[1] for i,v in (row[measurement_types]==m).items() if v == True]
                    if len(meas_ix)>0:
                        break
                
                col_num = list(set(var_ix) & set(meas_ix))
                if len(col_num)==1:
                    col_num = col_num[0]
                else:
                    col_num = '0'
                eddyProGroupDefs['Project'][key] = col_num

            file_prototype = self.fileInventory.loc[self.fileInventory['groupID']==groupID,'file_prototype'].values[0]
            eddyProCols = configparser.ConfigParser()
            eddyProCols.read_dict(eddyProGroupDefs)
                
            # Save the run and append to the list of runs
            filename = self.config['Paths']['meta_dir']+eval(self.config['groupFiles']['eddyProCols'])
            with open(filename, 'w') as eddypro:
                eddypro.write(';EDDYPRO_PROCESSING\n')
                eddyProCols.write(eddypro,space_around_delimiters=False)

    def filterData(self):
        print("Applying Filters:")
        # Alias to simplify eval statement definitions
        Data = self.rawDataStatistics.astype('float')
        for name,rule in self.config['monitoringInstructions']['dataFilters'].items():
            print(name,rule,'\n')
            for condition,parameters in rule.items():
                # Identify data columns corresponding to desired variable *or* measurement type
                col = self.configurationGroups['FileDescription'].apply(
                        lambda row: [ colnames[0].split('_')[1] for colnames in
                            list(row[((row.isin(parameters['variables']))|(row.isin([parameters['measure_type']])
                                    ))].index.values)],axis=1)
                # Reduce to the column corresponding to desired variable *and* measurement type
                header_names = col.apply(lambda lst: [
                    'col_'+k+'_header_name' for k,c in zip(Counter(lst).keys(),Counter(lst).values())
                    if c == max(Counter(lst).values())])
                # For every unique configuration, apply the filtering rule
                for (groupID,groupRow),headers in zip(self.configurationGroups.iterrows(),header_names):
                    # Get column names corresponding to query
                    h = groupRow.loc[pd.IndexSlice[['Custom'],headers,['first']]].values
                    # Get rows corresponding to groups
                    groupIX = (Data.loc[:,pd.IndexSlice[['group'],['ID']]]==groupID).max(axis=1).values
                    for stat,filter in parameters['filters'].items():
                        variables = pd.IndexSlice[h,:,[stat]]
                        if self.config['stringTags']['NaN'] not in h:
                            test = eval(filter).max(axis=1)
                            flag = test[test==True].index
                            # Add a filter flag to exclude timestamps from EddyPro runs and list the corresponding exclusion condition
                            self.fileInventory.loc[flag.values,'Filter Flags'] = (self.fileInventory.loc[flag.values,'Filter Flags'].str.replace(self.config['stringTags']['NaN'],'')+f',{name}: {condition}').str.lstrip(',')
                        else:
                            self.fileInventory.loc[self.fileInventory['groupID']==groupID,'Filter Flags'] = (self.fileInventory.loc[self.fileInventory['groupID']==groupID,'Filter Flags'].str.replace(self.config['stringTags']['NaN'],'')+f',{name}: Data not available').str.lstrip(',')

    # File names are modified to facilitate EddyPro processing them by group
    def renameFiles(self):
        # Loop through flagged records and prepend the filename with an exclusion tag
        for i,row in self.fileInventory.loc[
            ((self.fileInventory['Filter Flags']!=self.config['stringTags']['NaN'])&
            (self.fileInventory['filename'].str.startswith(self.config['stringTags']['exclude'])==False))
            ].iterrows():
            old_fn = row['filename']
            new_fn = self.config['stringTags']['exclude']+row['filename']
            dpth = f"{self.config['Paths']['raw']}/{i.year}/{str(i.month).zfill(2)}/"
            if os.path.isfile(f"{dpth}/{new_fn}"):
                print(f"renaming for exclusion: {old_fn} to {new_fn}")
                os.remove(f"{dpth}/{new_fn}")
            os.rename(f"{dpth}/{old_fn}",f"{dpth}/{new_fn}")
            self.fileInventory.loc[self.fileInventory.index==i,'filename']=new_fn
        # Loop through good records and prepend the filename with the group name
        for i,row in self.fileInventory.loc[(
            (self.fileInventory['Filter Flags']==self.config['stringTags']['NaN'])&
            (self.fileInventory['filename'].str.match(self.genericID)==False)&
            (self.fileInventory['filename']!=self.config['stringTags']['NaN'])
            )].iterrows():
            groupID = row['groupID']
            old_fn = row['filename']
            file_prototype = eval(self.config['stringTags']['groupID'])+row['file_prototype']
            new_fn = eval(self.config['stringTags']['groupID'])+row['filename']
            dpth = f"{self.config['Paths']['raw']}/{i.year}/{str(i.month).zfill(2)}/"
            self.fileInventory.loc[self.fileInventory.index==i,['filename','file_prototype']]=new_fn,file_prototype
            if os.path.isfile(f"{dpth}/{new_fn}"):
                os.remove(f"{dpth}/{new_fn}")
            os.rename(f"{dpth}/{old_fn}",f"{dpth}/{new_fn}")      
        self.saveMetadataFiles()

    def saveMetadataFiles(self):
        # Save the revised inventory
        self.fileInventory.to_csv(self.config['fileInventory'])
        self.rawDataStatistics.to_csv(self.config['rawDataStatistics'])
        self.metaDataValues.to_csv(self.config['metaDataValues'])
        self.configurationGroups.to_csv(self.config['configurationGroups'])
        
class runEP(eddyProAPI):
    def __init__(self,**kwargs):
        kwargs['reset'] = False
        if 'processes' not in kwargs:
            kwargs['processes']=int(os.cpu_count()/2)
        super().__init__(**kwargs)
        
        self.tempDir = os.path.abspath(os.getcwd()+'/temp/')
        if self.debug == False and os.path.isdir(self.tempDir):
            shutil.rmtree(self.tempDir)
            os.mkdir(self.tempDir)
            with open(os.getcwd()+'/temp/.gitignore', 'w') as ig:
                ig.write('*')
                
        self.fileInventory = self.fileInventory.dropna()
        ptype = ('Custom','file_prototype','first')
        self.rpList = []
        self.fccList = []
        self.ex_fileList = []    
        self.groupIDValues = [f"group_{id}" for id in self.configurationGroups.index]
        self.runEddyPro = batchProcessing.runEddyPro(self.config['Paths']['baseEddyPro'],
                        self.groupIDValues,self.priority,self.debug)
        
        for groupID,groupInfo in self.configurationGroups.loc[self.configurationGroups[ptype]!=self.config['stringTags']['exclude']].iterrows():
            groupTimeStamps = self.fileInventory.loc[self.fileInventory['groupID'].astype('int')==groupID].index
            groupTimeStamps = groupTimeStamps[((groupTimeStamps>=self.dateRange.min())&
                                               (groupTimeStamps<=self.dateRange.max()))]
            ix = pd.Series([i for i in range(groupTimeStamps.shape[0])])
            if ix.shape[0]>0:
                self.batchesPerGroup(ix.shape[0],groupID)
                bins = np.arange(0,self.nBatchesPerGroup+1)/self.nBatchesPerGroup
                labels = np.arange(1,self.nBatchesPerGroup+1).astype(np.int32)
                batches = pd.qcut(ix,q=bins,labels=labels)

                for id in batches.unique():
                    self.makeBatch(groupID,f"group_{groupID}_rp_{chr(ord('@')+id)}",
                                groupInfo,
                                groupTimeStamps[batches==id].min(),
                                groupTimeStamps[batches==id].max()+pd.Timedelta(minutes=int(groupInfo['Timing','file_duration','first'])),
                                groupTimeStamps[batches==id].shape[0])
                self.makeBatch(groupID,f"group_{groupID}_fcc",
                                groupInfo,
                                groupTimeStamps.min(),
                                groupTimeStamps.max()+pd.Timedelta(minutes=int(groupInfo['Timing','file_duration','first'])),
                                groupTimeStamps.shape[0])
        if len(self.rpList)<self.processes:self.processes = len(self.rpList)
        self.runGroups()

    def batchesPerGroup(self,nInGroup,groupID):
        self.minN = 1
        minN = 1
        for section,options in self.config['minBatchSizes'].items():
            for option, limit in options.items():
                if section in self.userDefinedEddyProSettings.keys() and option in self.userDefinedEddyProSettings[section].keys() and self.userDefinedEddyProSettings[section][option] in limit.keys():
                    minN = limit[self.userDefinedEddyProSettings[section][option]]
                elif self.eddyProStaticConfig[section][option] in limit.keys():
                    minN = limit[self.eddyProStaticConfig[section][option]]
                else:
                    minN = 1
            self.minN = max(self.minN,minN)
        self.nBatchesPerGroup = np.floor(nInGroup/self.minN)
        if self.nBatchesPerGroup < 1:
            print(f'Warning, available data in group {groupID} is below recommended size for selected settings.' )
        self.nBatchesPerGroup = max(1,self.nBatchesPerGroup)
        self.nBatchesPerGroup = min(self.processes,self.nBatchesPerGroup)

    def makeBatch(self,groupID,project_id,groupInfo,batchStart,batchEnd,batchCount):
        id = f'group_{groupID}'
        file_name = f"{self.tempDir}\{project_id}.eddypro"
        if '_rp_' in file_name:
            self.rpList.append(file_name)
            # Dump rp runs from subprocesses to root of group run
            out_path = self.runEddyPro.tempDir[id]
            ex_file = ''
            bin_sp_avail='0'
            sa_bin_spectra=''
            full_sp_avail='0'
            sa_full_spectra=''
        else:
            self.fccList.append(file_name)
            # Dump fcc runs to root of temp dir 
            out_path = self.tempDir
            ex_file = f"{self.runEddyPro.tempDir[id]}/eddypro_{project_id}_fluxnet.csv"
            sa_bin_spectra = f"{self.runEddyPro.tempDir[id]}/eddypro_binned_cospectra/"
            bin_sp_avail='1'
            sa_full_spectra = f"{self.runEddyPro.tempDir[id]}/eddypro_full_cospectra/"
            full_sp_avail='1'
            self.ex_fileList.append(ex_file)
        print(f'Creating {file_name} for {batchCount} files')
        proj_file = self.config['Paths']['meta_dir']+eval(self.config['groupFiles']['groupMetaData'])
        file_prototype = groupInfo['Custom','file_prototype','first']
        master_sonic = groupInfo['Instruments','instr_1_model','first']
        if file_prototype.endswith('.ghg'):
            file_type='0'
        else:
            file_type='1'
        pr_start_date=str(batchStart.date())
        pr_start_time=str(batchStart.time())[:5]
        pr_end_date=str(batchEnd.date())
        pr_end_time=str(batchEnd.time())[:5]
                       
        eddyProCols = configparser.ConfigParser()
        eddyProCols.read(
            self.config['Paths']['meta_dir']+eval(self.config['groupFiles']['eddyProCols'])
        )

        self.groupEddyProConfig = configparser.ConfigParser() 

        for section in self.eddyProStaticConfig.keys():
            for option,value in self.eddyProStaticConfig[section].items():
                if not self.groupEddyProConfig.has_section(section):
                    self.groupEddyProConfig.add_section(section)
                self.groupEddyProConfig.set(section, option, value)

                if eddyProCols.has_section(section) and eddyProCols.has_option(section, option):
                    self.groupEddyProConfig.set(section, option, eddyProCols[section][option])

                # Use evaluate statement for dynamic settings
                if self.eddyProDynamicConfig.has_section(section) and self.eddyProDynamicConfig.has_option(section, option):  
                    self.groupEddyProConfig.set(section, option, eval(self.eddyProDynamicConfig[section][option]))

                # User supplied variables will overwrite any other settings
                if section in self.userDefinedEddyProSettings.keys() and option in self.userDefinedEddyProSettings[section].keys():
                    self.groupEddyProConfig.set(section, option,str(self.userDefinedEddyProSettings[section][option]))
        # Save the run and append to the list of runs
        with open(file_name, 'w') as eddypro:
            eddypro.write(';EDDYPRO_PROCESSING\n')
            self.groupEddyProConfig.write(eddypro,space_around_delimiters=False)
                    
    def runGroups(self):
        print(f'Initiating EddyPro Runs for group on {self.processes} cores at {self.priority} priority')
        subProcesIDs = []
        if (__name__ == 'eddyProAPI' or __name__ == '__main__') and self.processes>1:
            # run routine in parallel
            pb = progressbar(len(self.rpList),'')
            with Pool(processes=self.processes) as pool:
                for out in pool.imap(self.runEddyPro.rpRun,self.rpList,chunksize=1):
                    pb.step()
                    subProcesIDs.append(out)
                pool.close()
                pb.close()
        else:
            # run routine sequentially for debugging
            for i,toRun in enumerate(self.rpList):
                out = self.runEddyPro.rpRun(toRun)
                subProcesIDs.append(out)
        self.rpMerge()
        for fcc in self.fccList:
            subProcesIDs.append(self.runEddyPro.fccRun(fcc))
        self.copyFinalOutputs(subProcesIDs)
    
    def rpMerge(self):
        for groupID in self.configurationGroups.index:
            for filePattern,kwargs in self.config['rpIntermediary'].items():
                id = f'group_{groupID}'
                search_path = os.path.abspath(f"{self.runEddyPro.tempDir[id]}/**{filePattern}**.csv")
                toMerge = glob(search_path)
                if len(toMerge)>0:
                    Temp = pd.DataFrame()
                    if 'parse_dates' in kwargs and type(kwargs['parse_dates'])==list:
                        val = kwargs['parse_dates']
                        key = tuple(['datetime']+['' for i in range(len(kwargs['header'])-1)])
                        kwargs['parse_dates'] = {key:val}
                    for i,fn in enumerate(toMerge):
                        Temp = pd.concat([Temp,pd.read_csv(fn,**kwargs)])
                    print(self.config['rpIntermediary'].items())
                    Temp = Temp.set_index(list(kwargs['parse_dates'].keys())[0]).sort_index()
                    Temp = Temp.sort_index()
                    fn = [f for f in self.ex_fileList if id in f][0].replace('fluxnet',filePattern)
                    print('Saving As')
                    print(fn)
                    Temp.to_csv(fn,index=False)

    def copyFinalOutputs(self,subProcesIDs):
        print(subProcesIDs)
        for toDel in subProcesIDs:
            print(toDel)
            shutil.rmtree(toDel)
        d_out = os.path.abspath(self.config['Paths']['output_path'])
        d_out_rn = os.path.abspath(d_out+'/'+datetime.strftime(datetime.now(),format='%Y%m%d%H%M'))
        d_in = os.path.abspath(f"{os.getcwd()}/temp/")
        batchProcessing.pasteWithSubprocess(d_in,d_out,option='move')
        os.rename(os.path.abspath(d_out+'/temp'),d_out_rn)
        if self.biometUser and os.path.isdir(self.config['BiometUser']['Biomet.net']):
            for outFile,metaData in self.config['fccFinalOutputs'].items():
                toDump = fnmatch.filter(os.listdir(d_out_rn),f'*{outFile}*')
                for td in toDump:
                    print(td)
                    dumpToBiometDatabase(siteID=self.siteID,
                                        biometPath = self.config['BiometUser']['Biomet.net'],
                                        database = self.config['BiometUser']['Database'],
                                        inputFile=f"{d_out_rn}/{td}",
                                        metaData=metaData,
                                        stage='epAutoRun',
                                        tag=self.name)

# If called from command line ...
if __name__ == '__main__':

    # Parse the arguments
    CLI=argparse.ArgumentParser()

    dictArgs = []
    for key,val in defaultArgs.items():
        dt = type(val)
        nargs = "?"
        if dt == type({}):
            dictArgs.append(key)
            dt = type('')
            val = '{}'
        elif dt == type([]):
            nargs = '+'
            dt = type('')
        CLI.add_argument(f"--{key}",nargs=nargs,type=dt,default=val)

    CLI.add_argument("--runMode",nargs='?',type=str,default='full')

    # parse the command line
    args = CLI.parse_args()

    kwargs = vars(args)
    for d in dictArgs:
        kwargs[d] = json.loads(kwargs[d])
    if kwargs['runMode'] != '2':
        preProcessing(**kwargs)
    if kwargs['runMode'] != '1':
        runEP(**kwargs)
