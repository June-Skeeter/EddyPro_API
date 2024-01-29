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
from subPath import sub_path
from multiprocessing import Pool
from HelperFunctions import progressbar
import parseFile
import sys
import importlib
importlib.reload(parseFile)

class read_ALL():
    def __init__(self,SiteID,Year,Month,processes=1,Test=0,reset=0):

        self.Year = str(Year)
        self.Month = "{:02d}".format(Month)
        self.SiteID = SiteID

        # Concatenate and read the ini files
        inis = ['configuration.ini','Metadata_Instructions.ini']
        ini_file = ['ini_files/'+ini for ini in inis]
        self.ini = configparser.ConfigParser()
        self.ini.read(ini_file)
        
        # self.MetadataTemplate = configparser.ConfigParser()
        # Create directories from template (see configuration.ini)
        self.ini['Paths']['dpath'] = sub_path(self,self.ini['Paths']['raw'])
        self.ini['Paths']['meta_dir'] = sub_path(self,self.ini['Paths']['metadata'])
        self.ini['Paths']['biomet_data'] = sub_path(self,self.ini['Paths']['biomet']+self.ini['filenames']['biomet'])
        
        self.ini['templates']['UpdateMetadata'] = self.ini['templates']['UpdateMetadata'].replace('METADATA',self.ini['Paths']['meta_dir'])

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
        for file in os.listdir(self.ini['Paths']['dpath']):
            name, tag = file.rsplit('.',1)
            if (tag == 'ghg' or tag == 'dat'):
                all_files.append(file)
                dates.append(pd.to_datetime(re.search(self.ini['FileName_DateFormat'][tag], name)[0].replace('_','-')))

        # Create dataframe of all GHG files
        df = pd.DataFrame(data={'filename':all_files,'TIMESTAMP':dates})
        df = df.set_index('TIMESTAMP')
        df['Flag']='-'
        df['MetaDataFile'] = '-'
        df['Update']='-'
        df['setup_ID']=np.nan
        
        # Flag timestamps with incomplete records
        df.loc[(((df.index.minute!=30)&(df.index.minute!=0))|(df.index.second!=0)),'Flag'] = 'Incomplete Record'
        for i,row in df.loc[((df['Flag']=='Incomplete Record')&(~df['filename'].str.contains('_incomplete')))].iterrows():
            old_fn = row['filename']
            new_fn = df.loc[df.index==i,'filename']=old_fn.split('.')[0]+'_incomplete.'+old_fn.split('.')[1]
            os.rename(f"{self.ini['Paths']['dpath']}/{old_fn}",f"{self.ini['Paths']['dpath']}/{new_fn}")
            
        # Resample to get timestamp on consistent half-hourly intervals
        df = df.resample('30T').first()
        # Write new file or append to existing file
        if reset == 1 or os.path.isfile(self.ini['Paths']['meta_dir']+self.ini['filenames']['file_inventory']) == False:
            # Sort so that oldest files get processed first
            self.files = df.sort_index()#ascending=False)
            self.files.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['file_inventory'])
        else:
            self.files = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['file_inventory'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.files = pd.concat([self.files,df.loc[df.index.isin(self.files.index)==False]])
            # Sort so that oldest files get processed first
            self.files = self.files.sort_index()#ascending=False)

    def makeEmpty(self,type='object',ixName='TIMESTAMP'):
        # Make an empty dataframe
        empty = pd.DataFrame(dtype=type)
        empty.index.name=ixName
        return(empty)
    
    def Read(self,processes,Test,reset):
        
        # Read existing data records if they exist, create empty ones if they don't exist
        if reset == 0 and os.path.isfile(self.ini['Paths']['meta_dir']+self.ini['filenames']['file_inventory']):
            self.dataRecords = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['raw_means'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.dynamic_metadata = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['dynamic_metadata'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.site_setup = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['site_setup'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.Calibration = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['calibration_parameters'])
        else:
            self.dataRecords = self.makeEmpty(type='float')
            self.dynamic_metadata = self.makeEmpty(type='float')
            self.site_setup = self.makeEmpty(type='float')
            self.Calibration = self.makeEmpty()#ixName='TIMESTAMP')

        # Empty log for current run
        self.Log = self.makeEmpty()

        if Test > 0:
            self.to_Process = self.files.loc[self.files['setup_ID'].isna()][:Test]
        else:
            self.to_Process = self.files.loc[self.files['setup_ID'].isna()]

        NameList = self.to_Process['filename'].values
        TimeList = self.to_Process.index

        if reset == 1:
            # Run once to get the first Metadata file and use it as a template
            self.Parser.process_file([NameList[0],TimeList[0]],Template_File=False)
        # else:
        #     # Run once to get the first Metadata file and use it as a template
        #     Template_File = self.files.loc[self.files['MetaDataFile']!='-','MetaDataFile'].values[-1]
        #     print(Template_File)
        #     self.Parser.process_file([NameList[0],TimeList[0]],Template_File=self.ini['Paths']['meta_dir']+Template_File)


        # Use parallel processing to speed things up unless otherwise instructed
        if (__name__ == 'preProcessing' or __name__ == '__main__') and processes>1:
            pb = progressbar(len(NameList),'Preprocessing Files')
            with Pool(processes=processes) as pool:

                # Break year into one-day chunks
                max_chunksize=48
                chunksize=min(int(np.ceil(len(NameList)/processes)),max_chunksize)
                
                for out in pool.imap(self.Parser.process_file,zip(NameList,TimeList),chunksize=chunksize):
                    self.appendRecs(out)
                    pb.step()
                pool.close()

        # Sequential processing is helpful for trouble shooting but will run much slower
        else:
            for fn,ts in zip(NameList,TimeList):
                out = self.Parser.process_file([fn,ts],Testing=True)
                self.appendRecs(out)
                self.out = out

        self.prepareOutputs()        
    
    def appendRecs(self,out):
        df = pd.DataFrame(data=out['dataValues'],index=[out['TimeStamp']])
        df.index.name='TIMESTAMP'

        dyn = self.ini['Monitor']['dynamic_metadata'].split(',')
        self.dynamic_metadata = pd.concat([self.dynamic_metadata,df[dyn]])
        stup = self.ini['Monitor']['site_setup'].split(',')
        self.site_setup = pd.concat([self.site_setup,df[stup]])

        self.dataRecords = pd.concat([self.dataRecords,df.loc[:,df.columns.isin(dyn+stup)==False]])
        self.files['Flag'] = self.files['Flag'].replace('-',np.nan)
        self.files['Flag'] = self.files['Flag'].fillna(out['Flag'])
        self.files['Flag'] = self.files['Flag'].fillna('-')
        self.files['Update'] = self.files['Update'].replace('-',np.nan)
        self.files['Update'] = self.files['Update'].fillna(out['Update'])
        self.files['Update'] = self.files['Update'].fillna('-')
        self.files.loc[self.files.index==out['TimeStamp'],'MetaDataFile'] = out['MetadataFile']
        self.Calibration = pd.concat([self.Calibration,out['calData']])
    
    def prepareOutputs(self):
        self.GroupCommon()
        self.dataRecords.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['raw_means'])
        self.dynamic_metadata.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['dynamic_metadata'])
        self.site_setup.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['site_setup'])
        self.Calibration.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['calibration_parameters'])
        self.GroupRuns()
        self.files = self.files.join(self.Logs)
        self.files.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['file_inventory'])

    def GroupCommon(self):
        self.site_setup['temp_ID'] = self.site_setup.reset_index().index
        self.site_setup['temp_ID'] = self.site_setup.groupby(self.ini['Monitor']['site_setup'].split(','))[['temp_ID']].transform(lambda x: x.min())
        self.site_setup['setup_ID']=np.nan
        for i,val in enumerate(self.site_setup['temp_ID'].unique()):
            self.site_setup.loc[self.site_setup['temp_ID']==val,'setup_ID'] = i
        self.files.loc[self.site_setup.index,'setup_ID']=self.files.loc[self.site_setup.index,'setup_ID'].index.map(self.site_setup['setup_ID'])
        self.site_setup = self.site_setup.drop_duplicates(keep='first')
        self.Calibration = self.Calibration.drop_duplicates(keep='first')

    def GroupRuns(self):
        Updates = self.files.loc[self.files['Update']!='-'].copy()
        Updates['dup'] = Updates['Update'].duplicated()
        toRemove = Updates.loc[Updates['dup']==True,'MetaDataFile'].values
        for rem in toRemove:
            self.files.loc[self.files['MetaDataFile']==rem,'MetaDataFile']=np.nan
            if os.path.isfile(f"{self.ini['Paths']['meta_dir']}{rem}"):
                os.remove(f"{self.ini['Paths']['meta_dir']}{rem}")
            if os.path.isfile(f"{self.ini['Paths']['meta_dir']}{rem.replace('.metadata','.eddypro')}"):
                os.remove(f"{self.ini['Paths']['meta_dir']}{rem.replace('.metadata','.eddypro')}")
        
        Updates.loc[Updates['dup']==True,['Update','MetaDataFile']]=np.nan
        Updates['Update'].fillna('-',inplace=True)
        Updates['MetaDataFile'].ffill(inplace=True)
        self.files.loc[self.files['Update']!='-',['Update','MetaDataFile']] = Updates[['Update','MetaDataFile']]
        self.files['MetaDataFile'].ffill(inplace=True)
        
    # def Report(self):


# If called from command line ...
if __name__ == '__main__':
    
    # Set to cwd to location of the current script
    Home_Dir = os.path.dirname(sys.argv[0])
    os.chdir(Home_Dir)

    # Parse the arguments
    CLI=argparse.ArgumentParser()

    CLI.add_argument(
    "--SiteID", 
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
    "--Test",
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
    
    read_ALL(args.SiteID[0],args.year[0],args.processes,args.Test,args.reset)

