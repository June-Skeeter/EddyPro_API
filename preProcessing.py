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
from HelperFunctions import sub_path
from multiprocessing import Pool
from HelperFunctions import progressbar
import parseFile
import sys
import importlib
from datetime import datetime
import time
importlib.reload(parseFile)

class read_ALL():
    def __init__(self,siteID,Year,Month,processes=1,reset=0,Test=0,file_type='ghg',copy_From=None,copy_tag='',metadata_template=[],metadata_updates=None):
        self.file_type=file_type
        self.year = str(Year)
        self.month = "{:02d}".format(Month)
        self.siteID = siteID
        self.copy_From = copy_From
        self.copy_tag = copy_tag

        # Concatenate and read the ini files
        inis = ['configuration.ini','Metadata_Instructions.ini','EP_Dynamic_Updates.ini']
        ini_file = ['ini_files/'+ini for ini in inis]
        self.ini = configparser.ConfigParser()
        self.ini.read(ini_file)
        self.read_data_dir = sub_path(self,self.ini['Paths']['read_data_dir'])
        
        # Create directories from template (see configuration.ini)
        self.ini['Paths']['dpath'] = sub_path(self,self.ini['Paths']['raw'])
        self.ini['Paths']['meta_dir'] = sub_path(self,self.ini['Paths']['metadata'])
        self.ini['Paths']['biomet_data'] = sub_path(self,self.ini['Paths']['biomet']+self.ini['filenames']['biomet'])
        if metadata_updates is not None:
            self.ini['filenames']['metadata_to_overwrite'] = sub_path(self,metadata_updates)
        

        if reset == 1:
            proceed = input(f"Warning: You are about to complete a hard reset, deleting all contents of: {self.ini['Paths']['meta_dir']}\nDo you wish to proceed? Y/N")
            if proceed.lower() == 'y' and os.path.isdir(self.ini['Paths']['meta_dir']):
                print(f"Deleting all contents of: {self.ini['Paths']['meta_dir']}")
                shutil.rmtree(self.ini['Paths']['meta_dir'])
                time.sleep(1)
            else:
                print(f"Keeping contents of: {self.ini['Paths']['meta_dir']}")
            
        if not os.path.exists(self.ini['Paths']['meta_dir']):
            Path(self.ini['Paths']['meta_dir']).mkdir(parents=True, exist_ok=True)
            time.sleep(1)

        # Copy over any metadata templates
        for md in metadata_template:
            if not os.path.isfile(self.ini['Paths']['meta_dir']+os.path.basename(md)):
                print('Copying Metadata file:\n',md)
                shutil.copy2(md,self.ini['Paths']['meta_dir'])
        
        time.sleep(1)
        self.Logs = []
        
        # Check for new .files - or overwrite if reset flag is set to true
        self.find_files(reset)
        self.Parser = parseFile.Parse(self.ini)
        if hasattr(self, 'files'):
            if self.files.loc[self.files.index.month==int(self.month)].size>0:
                self.Read(processes,Test,reset)


    def find_files (self,reset=0):
        # Find the name of every FULL file in the raw data folder located at the end of directory tree
        # Exclude any files that fall off half hourly intervals ie. maintenance
        # Resample to 30 min intervals - missing filenames will be null
        # Save the list of files

        # Get every file that is at the end of a directory tree
        all_files = []
        TIMESTAMP = []
        name_pattern = []
        if self.copy_From is not None:
            if os.path.isdir(self.ini['Paths']['dpath']) == False:
                os.makedirs(self.ini['Paths']['dpath'])
            self.setup()
        if os.path.isdir(self.ini['Paths']['dpath']):
            for file in os.listdir(self.ini['Paths']['dpath']):
                if file.endswith(self.file_type):
                    name = file.rsplit('.',1)[0]
                    all_files.append(file)
                    timestamp_info = re.search(self.ini[self.file_type]['search'], name).group(0)
                    TIMESTAMP.append(datetime.strptime(timestamp_info,self.ini[self.file_type]['format']))
                    name_pattern.append(name.replace(timestamp_info,self.ini[self.file_type]['ep_date_pattern'])+'.'+self.file_type)
            if len(all_files)>0:
                # Create dataframe of all GHG files
                df = pd.DataFrame(data={'filename':all_files,'TIMESTAMP':TIMESTAMP,'name_pattern':name_pattern})
                df = df.set_index('TIMESTAMP')
                df['Flag']=''
                df['MetaDataFile'] = ''
                df['Update']=''
                df['setup_ID']=np.nan
                # Flag timestamps with incomplete records
                df.loc[(((df.index.minute!=30)&(df.index.minute!=0))|(df.index.second!=0)),'Flag'] = 'Incomplete Record'
                for i,row in df.loc[((df['Flag']=='Incomplete Record')&(~df['filename'].str.contains('_incomplete')))].iterrows():
                    old_fn = row['filename']
                    new_fn = df.loc[df.index==i,'filename']=old_fn.split('.')[0]+'_incomplete.'+old_fn.split('.')[1]
                    if os.path.isfile(f"{self.ini['Paths']['dpath']}/{new_fn}"):
                        os.remove(f"{self.ini['Paths']['dpath']}/{new_fn}")
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
                    # Resample to get timestamp on consistent half-hourly intervals
                    self.files = self.files.resample('30T').asfreq()
                    # Sort so that oldest files get processed first
                    self.files = self.files.sort_index()#ascending=False)
        else:
            print(f"Not a valid directory: {self.ini['Paths']['dpath']}")

    def setup(self):
        for dir, _, files in os.walk(self.copy_From):
            if len(files)>0:
                pb = progressbar(len(files),'Copying Files')
                for file in files:
                    pb.step()
                    if file.endswith(self.file_type) and self.copy_tag in file and os.path.isfile(f"{self.ini['Paths']['dpath']}/{file}") == False:
                        srch = re.search(self.ini[self.file_type]['search'], file).group(0)
                        if srch is not None:
                            TIMESTAMP =  datetime.strptime(srch,self.ini[self.file_type]['format'])
                            if str(TIMESTAMP.year) == self.year and str(TIMESTAMP.month).zfill(2) == self.month:
                                shutil.copy(f"{dir}/{file}",f"{self.ini['Paths']['dpath']}/{file}")
                pb.close()


    def makeEmpty(self,type='object',ixName='TIMESTAMP'):
        # Make an empty dataframe
        empty = pd.DataFrame(dtype=type)
        empty.index.name=ixName
        return(empty)
    
    def Read(self,processes,Test,reset):
        # Read existing data records if they exist, create empty ones if they don't exist
        if reset == 0 and os.path.isfile(self.ini['Paths']['meta_dir']+self.ini['filenames']['raw_means']):
            self.dataRecords = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['raw_means'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.dynamic_metadata = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['dynamic_metadata'],parse_dates={'TIMESTAMP':['date','time']},index_col='TIMESTAMP')
            self.site_setup = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['site_setup'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
            self.Calibration = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['calibration_parameters'])
        else:
            self.dataRecords = self.makeEmpty(type='float')
            self.dynamic_metadata = self.makeEmpty(type='float')
            self.site_setup = self.makeEmpty(type='float')
            self.Calibration = self.makeEmpty()

        # Empty log for current run
        self.Log = self.makeEmpty()

        self.to_Process = self.files.loc[
            (
            (self.files['filename'].isnull()==False)&
            self.files['setup_ID'].isna()&
            self.files['filename'].str.endswith(self.file_type)&
            (self.files['filename'].str.contains('incomplete')==False)
            )]
        if Test > 0:
            self.to_Process = self.to_Process.iloc[:Test]
        NameList = self.to_Process['filename'].values
        TimeList = self.to_Process.index

        if self.files.loc[self.files['MetaDataFile'].fillna('').str.contains('.metadata')].size==0:
            # Run once to get the first Metadata file and use it as a template
            self.Parser.process_file([NameList[0],TimeList[0]],Template_File=False)
        if len(NameList)>0:
            if (__name__ == 'preProcessing' or __name__ == '__main__') and processes>1:
                pb = progressbar(len(NameList),f'Preprocessing {self.year} {self.month}')
                with Pool(processes=processes) as pool:

                    # Break request into one-day chunks
                    max_chunksize=48
                    chunksize=min(int(np.ceil(len(NameList)/processes)),max_chunksize)
                    for out in pool.imap(self.Parser.process_file,zip(NameList,TimeList),chunksize=chunksize):
                        self.appendRecs(out)
                        pb.step()
                    pool.close()
                    pb.close()
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
        stup = [x for x in stup if x in df.columns.values]
        self.ini['Monitor']['site_setup'] = ','.join(stup)
        self.site_setup = pd.concat([self.site_setup,df[stup]])

        self.dataRecords = pd.concat([self.dataRecords,df.loc[:,df.columns.isin(dyn+stup)==False]])
        self.files.loc[out['TimeStamp'],'Flag']=out['Log']['Flag']
        self.files.loc[out['TimeStamp'],'Update']=out['Log']['Update']
        self.files.loc[self.files.index==out['TimeStamp'],'MetaDataFile'] = out['MetadataFile']
        self.Calibration = pd.concat([self.Calibration,out['calData']])
    
    def prepareOutputs(self):
        self.GroupCommon()
        dyn = self.ini['Monitor']['dynamic_metadata'].split(',')
        static = [d for d in dyn if d in self.ini['Monitor']['fixed_dynamic']]
        dyn = [d for d in dyn if d not in self.ini['Monitor']['fixed_dynamic']]
        for s in static:
            self.dataRecords[s] = self.dynamic_metadata[s].mean()
            self.dataRecords[s+'_1SE'] = self.dynamic_metadata[s].std()/(self.dynamic_metadata[s].count()**.5)
        self.dataRecords.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['raw_means'])
        
        if self.ini['Monitor']['in_biomet_file'] != '':
            # Assumes using only one timestamp column in biomet.  EP does support multiple timestamp columns so could implement a more generic solution
            self.bm = pd.read_csv(self.ini['Paths']['biomet_data'],parse_dates=[self.ini['biom_timestamp']['name']],date_format=self.ini['biom_timestamp']['format'],index_col=self.ini['biom_timestamp']['name'],skiprows=[1])
            for param in self.ini['Monitor']['in_biomet_file'].split(','):
                if param in self.bm:
                    ow = self.bm.loc[self.bm.index.isin(self.dynamic_metadata.index),param]
                    self.dynamic_metadata.loc[self.dynamic_metadata.index.isin(ow.index),param] = ow
        self.dynamic_metadata['date'] = self.dynamic_metadata.index.strftime('%Y-%m-%d')
        self.dynamic_metadata['time'] = self.dynamic_metadata.index.strftime('%H:%M')
        dyn = ['date','time']+dyn
        self.dynamic_metadata[dyn].to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['dynamic_metadata'],index=False)
        self.site_setup.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['site_setup'])
        self.Calibration.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['calibration_parameters'])
        self.GroupRuns()
        self.files = self.files.join(self.Logs)
        self.files['filename']=self.files['filename'].fillna('')
        self.files.to_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['file_inventory'])

    def GroupCommon(self):
        self.site_setup['temp_ID'] = self.site_setup.reset_index().index
        self.site_setup = self.site_setup.fillna(0)
        self.site_setup['temp_ID'] = self.site_setup.groupby(self.ini['Monitor']['site_setup'].split(','))[['temp_ID']].transform(lambda x: x.min())
        self.site_setup['setup_ID']=np.nan
        for i,val in enumerate(self.site_setup['temp_ID'].unique()):
            self.site_setup.loc[self.site_setup['temp_ID']==val,'setup_ID'] = i
        self.files.loc[self.site_setup.index,'setup_ID']=self.files.loc[self.site_setup.index,'setup_ID'].index.map(self.site_setup['setup_ID'])
        self.site_setup = self.site_setup.drop_duplicates(keep='first')
        self.Calibration = self.Calibration.drop_duplicates(keep='first')

    def GroupRuns(self):
        Grp = self.files.groupby(['Flag','setup_ID','name_pattern']).first()[['MetaDataFile']]
        mdKeep = Grp['MetaDataFile'].values
        toRemove = self.files.loc[self.files['MetaDataFile'].isin(mdKeep)==False,'MetaDataFile'].dropna().unique()
        for i,row in Grp.iterrows():
            self.files.loc[(
                (self.files['Flag']==i[0])&
                (self.files['setup_ID']==i[1])&
                (self.files['name_pattern']==i[2])
                ),'MetaDataFile']=row['MetaDataFile']
        for rem in toRemove:
            if os.path.isfile(f"{self.ini['Paths']['meta_dir']}{rem}"):
                os.remove(f"{self.ini['Paths']['meta_dir']}{rem}")
            if os.path.isfile(f"{self.ini['Paths']['meta_dir']}{rem.replace('.metadata','.eddypro')}"):
                os.remove(f"{self.ini['Paths']['meta_dir']}{rem.replace('.metadata','.eddypro')}")
        
# If called from command line ...
if __name__ == '__main__':
    
    # Set to cwd to location of the current script
    Home_Dir = os.path.dirname(sys.argv[0])
    os.chdir(Home_Dir)

    # Parse the arguments
    CLI=argparse.ArgumentParser()

    CLI.add_argument(
    "--siteID", 
    nargs='+', # 1 or more values expected => creates a list
    type=str,
    default=[],
    )

    CLI.add_argument(
    "--Years",
    nargs='+',
    type=int,  
    default=[datetime.now().year],
    )
    CLI.add_argument(
    "--Month",
    nargs='+',
    type=int,  
    default=[i+1 for i in range(12)],
    )

    CLI.add_argument(
    "--Processes",
    nargs="?",# Use "?" to limit to one argument instead of list of arguments
    type=int,  
    default=1,
    )
    
    # parse the command line
    args = CLI.parse_args()

    for site in args.siteID:
        
        for year in args.Years:

            for month in args.Month:
                print(f'Processing: {site} {year}/{month}')
                t1 = time.time()
                read_ALL(site,year,month,args.Processes)
                print(f'Completed in: {np.round(time.time()-t1,4)} seconds')

