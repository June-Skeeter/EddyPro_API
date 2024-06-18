# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import re
import sys
import yaml
import time
import shutil
import fnmatch
import argparse
import batchProcessing
import importlib
import numpy as np
import pandas as pd
import configparser
from pathlib import Path
from datetime import datetime,date
from functools import partial
from collections import Counter
from multiprocessing import Pool
from HelperFunctions import sub_path
from HelperFunctions import progressbar
importlib.reload(batchProcessing)

# Directory of current script
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)

class eddyProAPI():
    os.chdir(dname)
    def __init__(self,siteID,dateRange=None,fileType='ghg'):
        self.siteID = siteID
        if dateRange is not None:
            self.dateRange = pd.DatetimeIndex(dateRange)
        else:
            self.dateRange = pd.DatetimeIndex([date(datetime.now().year,1,1),datetime.now()])
        self.fileType = fileType



class preProcessing(eddyProAPI):
    def __init__(self,siteID,dateRange=None,fileType='ghg',
                 metaDataTemplate=None,eddyProTemplate='LabStandard_Advanced.eddypro',
                 processes=os.cpu_count(),Testing=0,reset=False):
        super().__init__(siteID,dateRange,fileType,)

        self.Testing = Testing

        # Turn of multiprocessing when testing
        if self.Testing > 0: self.processes = 1
        else: self.processes = processes
        
        # Concatenate and read the ini files
        # LICOR uses .ini format to define .metadata and .eddypro files
        EP_ini_files = [eddyProTemplate,'EP_Dynamic_Updates.ini']

        ini_file = ['ini_files/'+ini for ini in EP_ini_files]
        EP_ini = configparser.ConfigParser()
        EP_ini.read(ini_file)
        
        self.eddyProSettings = {key:dict(EP_ini[key]) for key in EP_ini.keys()}

        # Read yaml configurations
        with open('config_files/config.yml') as yml:
            self.config = yaml.safe_load(yml)
            self.config['siteID'] = siteID
            if os.path.isfile('config_files/user_path_definitions.yml'):
                with open('config_files/user_path_definitions.yml') as yml:
                    self.config.update(yaml.safe_load(yml))
            else:
                sys.exit(f"Missing {'config_files/user_path_definitions.yml'}")

        for f in ['monitoringInstructions']:
            if os.path.isfile(f'config_files/{f}.yml'):
                with open(f'config_files/{f}.yml') as yml:
                    self.config[f] = yaml.safe_load(yml)
            else:
                sys.exit(f"Missing {f'config_files/{f}.yml'}")

        # Setup paths using definitions from config file
        self.config['Paths'] = {}
        for key,val in self.config['RelativePaths'].items():
            self.config['Paths'][key] = eval(val)

        # Read the inventory if it exists already
        # Create the metadata_directory if it doesn't exist
        self.config['fileInventory']=self.config['Paths']['meta_dir']+self.config['filenames']['fileInventory']
        self.config['rawDataStatistics']=self.config['Paths']['meta_dir']+self.config['filenames']['rawDataStatistics']
        self.config['metaDataValues']=self.config['Paths']['meta_dir']+self.config['filenames']['metaDataValues']

        # Reset routine (for testing only, should be removed from production?)
        if reset == True: self.resetInventory()

        os.makedirs(self.config['Paths']['meta_dir'],exist_ok=True)
        os.makedirs(self.config['Paths']['raw'],exist_ok=True)

        if os.path.isfile(self.config['fileInventory']):
            self.fileInventory = pd.read_csv(self.config['fileInventory'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.rawDataStatistics = pd.read_csv(self.config['rawDataStatistics'],parse_dates=[0],index_col=[0],header=[0,1,2])
            self.metaDataValues = pd.read_csv(self.config['metaDataValues'],parse_dates=[0],index_col=[0],header=[0,1])
            
        # Initiate parser class
        # Defined externally to facilitate parallel processing
        self.Parser = batchProcessing.Parser(self.config,metaDataTemplate)
        
    def resetInventory(self):
        RESET = input(f"WARNING!! You are about to complete a reset: type SOFT RESET to delete all contents of: {self.config['Paths']['meta_dir']}, type HARD RESET to delete all contents of: {self.config['Paths']['meta_dir']} **and** {self.config['Paths']['raw']}\n, provide any other input + enter to exit the application")
        if RESET == 'SOFT RESET' or RESET == 'HARD RESET':
            if os.path.isdir(self.config['Paths']['meta_dir']):
                shutil.rmtree(self.config['Paths']['meta_dir'])
                if RESET == 'HARD RESET' and os.path.isdir(self.config['Paths']['raw']):
                    CONFIRM = input(F"WARNING!! You selected {RESET}, type CONFRIM to proceed, provide any other input + enter to exit the application")
                    if CONFIRM == 'CONFIRM':
                        shutil.rmtree(self.config['Paths']['raw'])
                    else:
                        sys.exit('Quitting')
        else:
            sys.exit('Quitting')

    def searchRawDir(self,copyFrom=None,searchTag='',timeShift=None):
        # Build the file inventory of the "raw" directory and copy new files if needed
        # Option to shift the timestamp: copy data and rename using shifted timestamp
        # timeShift will only be applied to data copied from another directory
        search_dirs = [self.config['Paths']['raw']]
        shiftTime = [None]
        if copyFrom is not None:
            search_dirs.append(copyFrom)
            shiftTime.append(timeShift)

        T1 = time.time()
        fileInfo = self.config[self.fileType]
        fileInfo['searchTag'] = searchTag

        # Walk the search directories
        for search,timeShift in zip(search_dirs,shiftTime):
            fileInfo['timeShift'] = timeShift
            for dir, _, fileList in os.walk(search):
                # Exclude files that have already been processed from fileList
                if hasattr(self,'fileInventory'):
                    source_names = [os.path.basename(f) for f in self.fileInventory['source'].values]
                else:
                    source_names = []
                fileList = [f for f in fileList if f not in source_names]
                if len(fileList)>0:
                    print(f'Searching {dir}')
                    douot = []
                    if (__name__ == 'preProcessing' or __name__ == '__main__') and self.processes>1:
                        # run routine in parallel
                        pb = progressbar(len(fileList),'')
                        with Pool(processes=self.processes) as pool:
                            max_chunksize=10
                            chunksize=min(int(np.ceil(len(fileList)/self.processes)),max_chunksize)
                            for out in pool.imap(partial(batchProcessing.copy_and_check_files,in_dir=dir,out_dir=self.config['Paths']['raw'],fileInfo=fileInfo,dateRange=self.dateRange),fileList,chunksize=chunksize):
                                pb.step()
                                douot.append(out)
                            pool.close()
                            pb.close()
                    else:
                        # run routine sequentially for debugging
                        testOffset=0
                        for i,filename in enumerate(fileList):
                            if i < self.Testing + testOffset or self.Testing == 0:
                                out = batchProcessing.copy_and_check_files(filename,dir,self.config['Paths']['raw'],fileInfo=fileInfo,dateRange=self.dateRange)
                                print(out)
                                if out[0] is None and self.Testing != 0:
                                    testOffset += 1
                                douot.append(out)
                    # Dump results to inventory
                    # source and filename will be different if a timeShift is applied when copying
                    df = pd.DataFrame(columns=['TIMESTAMP','source','filename','name_pattern'],data=douot)
                    # Add empty columns for auxillary information
                    df[['Filter Flags']]=self.config['naString']
                    df['TIMESTAMP'] = pd.DatetimeIndex(df['TIMESTAMP'])
                    df = df.set_index('TIMESTAMP')
                    # drop rows where filename_final are missing
                    df = df.loc[df['filename'].isnull()==False]
                    # Merge with existing inventory
                    if hasattr(self,'fileInventory'):      
                        self.fileInventory = pd.concat([self.fileInventory,df])
                    else:
                        self.fileInventory = df
        
        # Quit if no data were found
        if self.fileInventory.empty:
            sys.exit('No Data Found')
        # Resample to get timestamp on consistent half-hourly intervals
        self.fileInventory = self.fileInventory.resample('30T').first()
        # Fill empty string columns
        self.fileInventory = self.fileInventory.fillna(self.config['naString'])

        # Sort so that oldest files get processed first
        self.fileInventory = self.fileInventory.sort_index()#ascending=False)
        # Save inventory
        self.fileInventory.to_csv(self.config['fileInventory'])
        print('Files Search Complete, time elapsed: ',time.time()-T1)
    
    def readFiles(self):
        T1 = time.time()
        print('Reading Data')
        # Parse down to just files that need to be read
        if self.dateRange is None:
            to_process = self.fileInventory.loc[self.fileInventory['filename'].str.endswith(self.fileType)].copy()
        else:
            to_process = self.fileInventory.loc[(
                (self.fileInventory['filename'].str.endswith(self.fileType))&
                (self.fileInventory.index>=self.dateRange.min())&(self.fileInventory.index<=self.dateRange.max())
                )].copy()

        # Call file handler to parse files in parallel (default) or sequentially for troubleshooting
        pathList=(self.config['Paths']['raw']+to_process.index.strftime('%Y/%m/')+to_process['filename'])
        self.rawDataStatistics = pd.DataFrame()
        self.metaDataValues = pd.DataFrame()
        if (__name__ == 'preProcessing' or __name__ == '__main__') and self.processes>1:
            # run routine in parallel
            pb = progressbar(len(pathList),'')
            with Pool(processes=self.processes) as pool:
                max_chunksize=4
                chunksize=min(int(np.ceil(len(pathList)/self.processes)),max_chunksize)
                for out in pool.imap(partial(self.Parser.readFile),pathList.items(),chunksize=chunksize):
                    pb.step()
                    self.rawDataStatistics = pd.concat([self.rawDataStatistics,out[0]])
                    self.metaDataValues = pd.concat([self.metaDataValues,out[1]])
                pool.close()
                pb.close()
        else:
            # run routine sequentially
            for i, (timestamp,file) in enumerate(pathList.items()):
                if i < self.Testing or self.Testing == 0:
                    T2 = time.time()
                    # if i < self.Testing or self.Testing == 0:
                    out = self.Parser.readFile((timestamp,file))
                    self.rawDataStatistics = pd.concat([self.rawDataStatistics,out[0]])
                    self.metaDataValues = pd.concat([self.metaDataValues,out[1]])
                    print(f'{file} complete, time elapsed: ',time.time()-T2)
        
        print('Reading Complete, time elapsed: ',time.time()-T1)
        
        self.rawDataStatistics.to_csv(self.config['rawDataStatistics'])
        self.metaDataValues.to_csv(self.config['metaDataValues'])

    def groupMetadata(self):
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
        
        self.metaDataValues[grouper] = self.metaDataValues[grouper].fillna(self.config['naString'])

        # Generate group labels based off unique configurations of groupBy values
        self.groupID = ('group','ID')
        groupLabels = pd.DataFrame(columns=pd.MultiIndex.from_tuples([self.groupID]),
                             data=(self.metaDataValues.groupby(by=grouper).grouper.group_info[0] + 1),
                             index = self.metaDataValues.index)
        
        self.metaDataValues = pd.concat([self.metaDataValues,groupLabels],axis=1)

        # get the ID values
        self.groupIDValues = groupLabels[self.groupID].unique()

        # Get statistics for tracked values by group
        track_cols = [self.groupID]+tracker
        self.groupStats = self.metaDataValues[track_cols].groupby(by=self.groupID).agg(self.Parser.agg)
        group_cols = [self.groupID]+grouper
        self.groupStats = self.groupStats.join(self.metaDataValues[group_cols].groupby(by=self.groupID).agg(['first','count']))
        
        # add the group labels to the statistics table
        groupLabels = groupLabels.T.set_index(np.repeat('', groupLabels.shape[1]), append=True).T
        self.rawDataStatistics = self.rawDataStatistics.join(groupLabels)

        groupLabels.columns=[''.join(col) for col in groupLabels.columns]
        self.fileInventory = self.fileInventory.join(groupLabels)


    def filterData(self):
        # Alias to simplify eval statement definitions
        Data = self.rawDataStatistics
        for name,rule in self.config['monitoringInstructions']['dataFilters'].items():
            for condition,parameters in rule.items():
                # Identify data columns corresponding to desired variable *or* measurement type
                col = self.groupStats['FileDescription'].apply(
                        lambda row: [ colnames[0].split('_')[1] for colnames in
                            list(
                                row[((row.isin(parameters['variables']))|
                                    (row.isin([parameters['measure_type']])
                                    ))].index.values)],
                        axis=1)
                # Reduce to the column corresponding to desired variable *and* measurement type
                header_names = col.apply(lambda lst: [
                    'col_'+k+'_header_name' for k,c in zip(Counter(lst).keys(),Counter(lst).values())
                    if c == max(Counter(lst).values())])
                # For every unique configuration, apply the filtering rule
                for (groupID,groupRow),headers in zip(self.groupStats.iterrows(),header_names):
                    # Get column names corresponding to query
                    h = groupRow.loc[pd.IndexSlice[['Custom'],headers,['first']]].values
                    # Get rows corresponding to groups
                    groupIX = (Data.loc[:,pd.IndexSlice[['group'],['ID']]]==groupID).max(axis=1).values
                    # Apply the filter with an eval statement
                    for stat,filter in parameters['filters'].items():
                        variables = pd.IndexSlice[h,:,[stat]]
                        test = eval(filter).max(axis=1)
                        flag = test[test==True].index
                        # Add a filter flag to exclude timestamps from EddyPro runs and list the corresponding exclusion condition
                        self.fileInventory.loc[flag.values,'Filter Flags'] = (self.fileInventory.loc[flag.values,'Filter Flags'].str.replace(self.config['naString'],'')+f',{name}: {condition}').str.lstrip(',')

    # Flagged file name are be modified to prevent EddyPro from processing them
    def excludeFlaggedFiles(self):
        # Loop through flagged records and edit the filename
        for i,row in self.fileInventory.loc[((self.fileInventory['Filter Flags']!=self.config['naString'])&
            (self.fileInventory['Filter Flags'].str.startswith(self.config['naString'])==False))].iterrows():
            old_fn = row['filename']
            new_fn = self.config['naString']+row['filename']
            dpth = f"{self.config['Paths']['raw']}/{i.year}/{str(i.month).zfill(2)}/"
            self.fileInventory.loc[self.fileInventory.index==i,'filename']=f"{dpth}{new_fn}"
            if os.path.isfile(f"{dpth}/{new_fn}"):
                print(f"renaming for exclusion: {old_fn} to {new_fn}")
                os.remove(f"{dpth}/{new_fn}")
            os.rename(f"{dpth}/{old_fn}",f"{dpth}/{new_fn}")

# class runEP(eddyProAPI):
#     def __init__(self,template,template_file,siteID,name=None,testing=False,processes=os.cpu_count(),priority = 'normal'):
        

# If called from command line ...
if __name__ == '__main__':
    
    # Set to cwd to location of the current script
    Home_Dir = os.path.dirname(sys.argv[0])
    os.chdir(Home_Dir)

    # Parse the arguments
    CLI=argparse.ArgumentParser()

    CLI.add_argument("--siteID",nargs='+',type=str,default=[],)
    
    CLI.add_argument("--fileType",nargs="?",type=str,default="ghg",)

    CLI.add_argument("--metadataUpdates",nargs="?",type=str,default=None,)
    
    CLI.add_argument("--metadataTemplate",nargs='+',type=str,default=[],)

    CLI.add_argument("--copyFrom",nargs="?",type=str,default=None,)

    CLI.add_argument("--searchTag",nargs="?",type=str,default='',)

    CLI.add_argument("--timeShift",nargs="?",type=int,default=None,)
  
    CLI.add_argument("--reset",nargs="?",type=int,default=0,)

    CLI.add_argument("--Years",nargs='+',type=int,default=[datetime.now().year],)

    CLI.add_argument("--Month",nargs='+',type=int,default=[i+1 for i in range(12)],)
    
    CLI.add_argument("--Processes",nargs="?",type=int,default=os.cpu_count(),)

    # parse the command line
    args = CLI.parse_args()

    for siteID in args.siteID:
        ra = read_ALL(siteID,fileType=args.fileType,metadataUpdates=args.metadataUpdates,copyFrom=args.copyFrom,searchTag=args.searchTag,timeShift=args.timeShift)
        for year in args.Years:

            for month in args.Month:
                print(f'Processing: {siteID} {year}/{month}')
                t1 = time.time()
                ra.find_files(year,month,args.Processes)
                print(f'Completed in: {np.round(time.time()-t1,4)} seconds')

