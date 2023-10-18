# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import re
import shutil
import argparse
import configparser
import numpy as np
import pandas as pd
from pathlib import Path
from functools import partial

from multiprocessing import Pool

from HelperFunctions import progressbar

import parseFile
# import eventLog

import sys

import importlib
importlib.reload(parseFile)

class read_ALL():
    def __init__(self,Site,Year,processes=1,Test=0,reset=0):

        # Concatenate the ini files
        inis = ['configuration.ini','Updates_Metadata.ini']
        ini_file = ['ini_files/'+ini for ini in inis]

        self.Year = str(Year)
        self.Site = Site

        self.ini = configparser.ConfigParser()
        self.ini.read(ini_file)
        
        self.MetadataTemplate = configparser.ConfigParser()
        
        self.ini['Paths']['dpath'] = self.sub(self.ini['Paths']['raw'])
        self.ini['Paths']['meta_dir'] = self.sub(self.ini['Paths']['metadata'])
        self.ini['Paths']['biomet_data'] = self.sub(self.ini['Paths']['biomet']+self.ini['filenames']['biomet'])
        if reset == 1:
            try:
                shutil.rmtree(self.ini['Paths']['meta_dir'])
            except:
                pass
            try:
                os.mkdir(self.ini['Paths']['meta_dir'])
            except:
                pass

        self.Parser = parseFile.Parse(self.ini)

        if not os.path.exists(self.ini['Paths']['meta_dir']):
            Path(self.ini['Paths']['meta_dir']).mkdir(parents=True, exist_ok=True)
        # self.EV = eventLog.EventLog()
        self.Logs = []
        
        # Check for new .ghg files - or overwrite if rest flag is set to true
        self.find_files(reset)
        self.Read(processes,Test,reset)

    def find_files (self,reset=0):
        # Find the name of every FULL .ghg or .dat file in the raw data folder located at the end of directory tree
        # Exclude any files that fall off half hourly intervals ie. maintenance
        # Resample to 30 min intervals - missing filenames will be null
        # Save the list of files

        # Get every .ghg or .dat file that is at the end of a directory tree
        all_files = []
        dates = []
        for (root, dir, files) in os.walk(self.ini['Paths']['dpath']):
            if root != self.ini['Paths']['dpath']:
                for file in files:
                    name, tag = file.rsplit('.',1)
                    if (tag == 'ghg' or tag == 'dat') and len(dir)==0:
                        all_files.append(file)
                        dates.append(pd.to_datetime(re.search(self.ini['FileName_DateFormat'][tag], name)[0].replace('_','-')))

        # Create dataframe of all GHG files
        df = pd.DataFrame(data={'filename':all_files,'TIMESTAMP':dates})
        df = df.set_index('TIMESTAMP')
        df['Flag']='-'
        df['MetaDataFile'] = '-'
        
        # Flag timestamps with incomplete records
        df.loc[(((df.index.minute!=30)&(df.index.minute!=0))|(df.index.second!=0)),'Flag'] = 'Incomplete Record'
        # Resample to get timestamp on consistent half-hourly intervals
        df = df.resample('30T').first()
        # Write new file or append to existing file
        if reset == 1 or os.path.isfile(self.ini['Paths']['meta_dir']+self.ini['filenames']['inventory']) == False:
            # Sort so that newest files get processed first
            self.files = df.sort_index(ascending=False)
            self.files.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['inventory'])
        else:
            self.files = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['inventory'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.files = pd.concat([self.files,df.loc[df.index.isin(self.files.index)==False]])
            # Sort so that newest files get processed first
            self.files = self.files.sort_index(ascending=False)

    def makeEmpty(self,type='object',ixName='TIMESTAMP'):
        # Make an empty dataframe
        empty = pd.DataFrame(dtype=type)
        empty.index.name=ixName
        return(empty)
    
    def sub(self,val):
        # update paths with relevant values
        v = val.replace('YEAR',str(self.Year)).replace('SITE',self.Site)
        return(v)
    
    def Read(self,processes,Test,reset):
        
        # Read existing data records if they exist, create empty ones if they don't
        if reset == 0 and os.path.isfile(self.ini['Paths']['meta_dir']+self.ini['filenames']['inventory']):
            self.dataRecords = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['raw_means'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.Calibration = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['calibration_parameters'])#,parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
        else:
            self.dataRecords = self.makeEmpty(type='float')
            self.Calibration = self.makeEmpty()#ixName='TIMESTAMP')

        # Empty log for current run
        self.Log = self.makeEmpty()

        if Test > 0:
            self.to_Process = self.files.loc[self.files['Flag']=='-'][:Test]
        else:
            self.to_Process = self.files.loc[self.files['Flag']=='-']
        NameList = self.to_Process['filename'].values
        TimeList = self.to_Process.index

        # Run once to get the first Metadata file and use it as a template
        self.Parser.process_file(NameList[0],TimeList[0],Template_File=False)
        
        # Use parallel processing to speed things up unless otherwise instructed
        if (__name__ == 'preProcessing' or __name__ == '__main__') and processes>1:
            pb = progressbar(len(NameList),'Preprocessing Files')
            with Pool(processes=processes) as pool:

                # Break year into one-day chunks
                max_chunksize=48
                chunksize=min(int(np.ceil(len(NameList)/processes)),max_chunksize)
                
                for i, result in enumerate(pool.starmap(self.Parser.process_file,zip(NameList,TimeList),chunksize=chunksize)):

                    self.appendRecs(result)
                    pb.step()
                pool.close()

        # Sequential processing is helpful for trouble shooting but will run much slower
        else:
            for fn,ts in zip(NameList,TimeList):
                out = self.Parser.process_file(fn,ts,Testing=True)
                self.appendRecs(out)
                self.out = out
            # self.dataRecords = out.dataRecords
            # self.files['Flag'] = out.EV.dfLog['Flag']


        self.dataRecords = self.dataRecords.drop_duplicates(keep='first')
        self.dataRecords.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['raw_means'])
        self.Calibration = self.Calibration.drop_duplicates(keep='first')
        # self.Calibration.reset_index(inplace=True,drop=True)
        self.Calibration.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['calibration_parameters'])

        self.files = self.files.join(self.Logs)
        # self.files['MetaDataFile']=self.files['MetaDataFile'].ffill()
        self.files.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['inventory'])
        # self.write_Daily(self.Variable_Columns.copy(),self.ini['filenames']['variable_columns'])
        
    
    def appendRecs(self,out):
        df = pd.DataFrame(data=out.dataValues,index=[out.TimeStamp])
        df.index.name='TIMESTAMP'
        self.dataRecords = pd.concat([self.dataRecords,df])
        self.files['Flag'] = self.files['Flag'].replace('-',np.nan)
        self.files['Flag'] = self.files['Flag'].fillna(out.EV.dfLog['Flag'])
        self.files['Flag'] = self.files['Flag'].fillna('-')
        self.files.loc[self.files.index==out.TimeStamp,'MetaDataFile'] = out.MetadataFile
        self.Calibration = pd.concat([self.Calibration,out.getCal.calData])

    def group_by_change(self,df,keys):
        return(df.groupby(keys).first())

    def find_Changes(self,df):
        df = df.dropna()
        nU = df.nunique()
        if nU.max() > 1:
            for col,val in nU.items():
                change_at = df.loc[df[col]!=df[col].shift(-1)].index
                for ca in change_at:
                    v = self.files.loc[ca,'MetaData Changed']
                    if v != v:
                        self.files.loc[ca,'MetaData Changed'] = col
                    else:
                        self.files.loc[ca,'MetaData Changed'] = v+'|'+col
    
    def setAssumptions(self,df,Assumptions):
        for key,val in Assumptions.items():
            df[key] = eval(val)
        return(df)

    def daily_Downsample(self,df,append_numeric=None):
        self.find_Changes(df)
        df = df.resample('D').first()
        if append_numeric is not None:
            for ap in append_numeric:
                df[ap] = self.dataRecords[ap].resample('D').mean()
        return(df)

    def write_Daily(self,df,name):
        df.index.name = 'date'
        self.find_Changes(df)
        cols = list(df.columns)
        cols.insert(0,'time')
        df['time'] = '00:00'
        df = df[cols]
        df.to_csv(f"{self.ini['Paths']['meta_dir']}{name}")



# If called from command line ...
if __name__ == '__main__':
    
    # Set to cwd to location of the current script
    Home_Dir = os.path.dirname(sys.argv[0])
    os.chdir(Home_Dir)

    # Parse the arguments
    CLI=argparse.ArgumentParser()

    CLI.add_argument(
    "--site", 
    nargs='+', # 1 or more values expected => creates a list
    type=str,
    default=['BB','BB2','BBS','RBM','DSM','HOGG','YOUNG'],
    )

    CLI.add_argument(
    "--year",
    nargs='+',
    type=int,  
    default=[],
    )

    CLI.add_argument(
    "--processes",
    nargs="?",# Use "?" to limit to one argument instead of list of arguments
    type=int,  
    default=1,
    )
    
    CLI.add_argument(
    "--test",
    nargs="?",
    type=int,  
    default=0,
    )

    CLI.add_argument(
    "--reset",
    nargs="?",
    type=int,  
    default=0,
    )
    

    # parse the command line
    args = CLI.parse_args()
    
    read_ALL(args.site[0],args.year[0],args.processes,args.test,args.reset)

