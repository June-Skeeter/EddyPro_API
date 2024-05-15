# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import re
import sys
import yaml
import time
import shutil
import argparse
import handleFiles
import importlib
import numpy as np
import pandas as pd
import configparser
import time
from pathlib import Path
from datetime import datetime
from functools import partial
from multiprocessing import Pool
from HelperFunctions import sub_path
from HelperFunctions import progressbar
importlib.reload(handleFiles)

# Directory of current script
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)

class preProcessing():
    os.chdir(dname)
    def __init__(self,siteID,fileType='ghg',
                 metaDataTemplate=None,eddyProTemplate='LabStandard_Advanced.eddypro',
                 processes=os.cpu_count(),Testing=0,reset=False):

        self.siteID = siteID
        self.fileType = fileType
        self.Testing = Testing
        # Turn of multiprocessing when testing
        if self.Testing > 0:
            self.processes = 1
        else:
            self.processes = processes
        
        # Concatenate and read the ini files
        # LICOR uses .ini format to define .metadata and .eddypro files
        EP_ini_files = [eddyProTemplate,'EP_Dynamic_Updates.ini']
        # {'default':eddyProTemplate,
        #                 'dynamicUpdates':'EP_Dynamic_Updates.ini'}

#         self.eddyProSettings = {
#             file:{key:dict(EP_ini[key]) for key in EP_ini.keys()} for 
#         }
# EP_ini_files

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
        if reset == True:
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

        os.makedirs(self.config['Paths']['meta_dir'],exist_ok=True)
        os.makedirs(self.config['Paths']['raw'],exist_ok=True)

        if os.path.isfile(self.config['fileInventory']):
            self.fileInventory = pd.read_csv(self.config['fileInventory'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.rawDataStatistics = pd.read_csv(self.config['rawDataStatistics'],parse_dates=[0],index_col=[0],header=[0,1,2])
            self.metaDataValues = pd.read_csv(self.config['metaDataValues'],parse_dates=[0],index_col=[0],header=[0,1])
            
        # Initiate parser class
        # Defined externally to facilitate parallel processing
        self.Parser = handleFiles.Parser(self.config,metaDataTemplate)

    def searchRawDir(self,copyFrom=None,searchTag='',timeShift=None):
        # Build the file inventory of the "raw" directory
        # Copy new files to raw if needed with option shift the timestamp 
        # e.g., copy data and rename using timestamp at end of interval instead of start
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
                            for out in pool.imap(partial(handleFiles.copy_and_check_files,in_dir=dir,out_dir=self.config['Paths']['raw'],fileInfo=fileInfo),fileList,chunksize=chunksize):
                                pb.step()
                                douot.append(out)
                            pool.close()
                            pb.close()
                    else:
                        # run routine sequentially
                        for i,filename in enumerate(fileList):
                            if i < self.Testing or self.Testing == 0:
                                out = handleFiles.copy_and_check_files(filename,dir,self.config['Paths']['raw'],fileInfo=fileInfo)
                                douot.append(out)
                    # Dump results to inventory
                    # source and filename will be different if a timeShift is applied when copying
                    df = pd.DataFrame(columns=['TIMESTAMP','source','filename','name_pattern'],data=douot)
                    # Add empty columns for auxillary information
                    df[['Flag','MetaDataFile','Update']]=''
                    df['setup_ID']=np.nan
                    df['TIMESTAMP'] = pd.DatetimeIndex(df['TIMESTAMP'])
                    df = df.set_index('TIMESTAMP')
                    # drop rows where filename_final are missing
                    df = df.loc[df['filename'].isnull()==False]
                    # Merge with existing inventory
                    if hasattr(self,'fileInventory'):      
                        self.fileInventory = pd.concat([self.fileInventory,df])
                    else:
                        # Quit if no data were found
                        if df.empty:
                            sys.exit('No Data Found')
                        self.fileInventory = df
        # Incomplete files should be excluded from inventory
        self.fullFilesOnly()
        # Resample to get timestamp on consistent half-hourly intervals
        self.fileInventory = self.fileInventory.resample('30T').first()
        # Fill empty string columns
        self.fileInventory[['source','filename','name_pattern']] = self.fileInventory[['source','filename','name_pattern']].fillna('N/A')

        # Sort so that oldest files get processed first
        self.fileInventory = self.fileInventory.sort_index()#ascending=False)
        # Save inventory
        self.fileInventory.to_csv(self.config['fileInventory'])
        print('Files Search Complete, time elapsed: ',time.time()-T1)

    def fullFilesOnly(self):
        # Flag timestamps with incomplete records
        self.fileInventory.loc[
            (((self.fileInventory.index.minute!=30)&(self.fileInventory.index.minute!=0))|
            (self.fileInventory.index.second!=0)),'Flag'] = 'Incomplete Record'
        # Loop through incomplete records and flag/rename
        for i,row in self.fileInventory.loc[((self.fileInventory['Flag']=='Incomplete Record')
                                &(self.fileInventory['filename'].str.contains('_incomplete')==False))].iterrows():
            old_fn = row['filename']
            dpth = f"{self.config['Paths']['raw']}/{i.year}/{str(i.month).zfill(2)}/"
            new_fn = self.fileInventory.loc[self.fileInventory.index==i,'filename']=old_fn.split('.')[0]+'_incomplete.'+old_fn.split('.')[1]
            if os.path.isfile(f"{dpth}/{new_fn}"):
                os.remove(f"{dpth}/{new_fn}")
            os.rename(f"{dpth}/{old_fn}",f"{dpth}/{new_fn}")
    
    def readFiles(self):
        T1 = time.time()
        print('Reading Data')
        # Parse down to just files that need to be read
        to_process = self.fileInventory.loc[self.fileInventory['filename'].str.endswith(self.fileType)].copy()
        # Call file handler to parse files in parallel (default) or sequentially for troubleshooting
        pathList=(self.config['Paths']['raw']+to_process.index.strftime('%Y/%m/')+to_process['filename'])
        self.rawDataStatistics = pd.DataFrame()
        self.metaDataValues = pd.DataFrame()
        md_out = []
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
            for i,file in pathList.items():
                T2 = time.time()
                # if i < self.Testing or self.Testing == 0:
                out = self.Parser.readFile((i,file))
                self.rawDataStatistics = pd.concat([self.rawDataStatistics,out[0]])
                self.metaDataValues = pd.concat([self.metaDataValues,out[1]])
                print(f'{file} complete, time elapsed: ',time.time()-T2)
        
        print('Reading Complete, time elapsed: ',time.time()-T1)
        
        self.rawDataStatistics.to_csv(self.config['rawDataStatistics'])
        self.metaDataValues.to_csv(self.config['metaDataValues'])

    # def inspectMetadata(self):
    #     for 



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

