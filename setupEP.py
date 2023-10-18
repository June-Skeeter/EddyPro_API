# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import sys
import runEP
import shutil
import configparser
import pandas as pd
from subprocess import run
from datetime import datetime
import multiprocessing
from multiprocessing import Pool

import importlib
importlib.reload(runEP)

from HelperFunctions import progressbar

class makeRun():

    def __init__(self,template_file,Site):
        
        inis = ['configuration.ini']#,'EP_Dynamic_Updates.ini']
        ini_file = ['ini_files/'+ini for ini in inis]
        self.ini = configparser.ConfigParser()
        self.ini.read(ini_file)
        
        self.Site = Site

        # Template file to be filled updated
        self.epRun = configparser.ConfigParser()
        self.epRun.read(template_file)

        # Parameters to update in template
        self.epUpdate = configparser.ConfigParser()
        self.epUpdate.read('ini_files/EP_Dynamic_Updates.ini')
        
        # Parameters to update in template
        self.epDataCols = configparser.ConfigParser()

        self.ini['Paths']['meta_dir'] = self.sub(self.ini['Paths']['metadata'])

        self.inventory = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['inventory'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
        self.inventory['MetaDataFile'] = self.inventory['MetaDataFile'].fillna('-')
        self.inventory = self.inventory.loc[self.inventory['MetaDataFile']!='-']

    def sub(self,val):
        v = val.replace('SITE',self.Site)
        if hasattr(self, 'Year'):
            v = v.replace('YEAR',str(self.Year))
        if hasattr(self, 'dateStr'):
            v = v.replace('DATE',str(self.dateStr))
        return(v)
    
    def runDates(self,dateRange,name,threads = 2,priority = 'normal'):
        runList = []
        Dates = (pd.date_range(dateRange[0],dateRange[1]))
        
        output_path = self.sub(self.ini['Paths']['eddypro_output']) 
        if os.path.isdir(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path)
        batch_path = output_path+'EP_Runs/'
        if os.path.isdir(batch_path):
            shutil.rmtree(batch_path)
        os.makedirs(batch_path)
        for runDate in Dates:
            self.dateStr = str(runDate.date())
            self.Year = runDate.year


            Metadata = self.inventory.loc[self.inventory.index.date==runDate.date(),'MetaDataFile'].unique()
            EddyProColumnUpdate = [f.replace('.metadata','.eddypro') for f in Metadata]
            if len(Metadata)>1:
                print('Wanning, Multiple MD for one day, implement fix')
            for Metadata_File,epcu in zip(Metadata,EddyProColumnUpdate):
                shutil.copy2(self.ini['Paths']['meta_dir']+Metadata_File,batch_path+Metadata_File)
                file_name = batch_path+self.dateStr+'.eddypro'
                self.epDataCols.read(self.ini['Paths']['meta_dir']+epcu)
                for section in self.epDataCols.keys(): 
                    for key,value in self.epDataCols[section].items():
                        self.epRun[section][key]=value
                for section in self.epUpdate.keys():
                    for key,value in self.epUpdate[section].items():
                        self.epRun[section][key]=eval(value)
                with open(file_name, 'w') as eddypro:
                    eddypro.write(';EDDYPRO_PROCESSING\n')
                    self.epRun.write(eddypro,space_around_delimiters=False)
                runList.append(file_name)    

        if (__name__ == 'setupEP' or __name__ == '__main__') :
            with Pool(processes=threads) as pool:
                threads = multiprocessing.active_children()
                
                for thread in threads:
                    cwd = os.getcwd()
                    bin = cwd+f'/temp/{thread.pid}/bin/'
                    ini = cwd+f'/temp/{thread.pid}/ini/'

                    shutil.copytree(self.ini['Paths']['eddypro_installation'],bin)

                    batchFile=f'{bin}runEddyPro.bat'.replace('/',"\\")
                    with open(batchFile, 'w') as batch:
                        contents = f'cd {bin}'
                        P = priority.upper().replace(' ','')
                        contents+=f'\nSTART /{P} '+self.ini['filenames']['eddypro_rp']
                        contents+='\n'+f'WMIC process where name="{self.ini["filenames"]["eddypro_rp"]}" CALL setpriority "{priority}"'
                        contents+='\nSTART '+self.ini['filenames']['eddypro_fcc']
                        contents+='\nEXIT'
                        batch.write(contents)
                    os.mkdir(ini)

                pb = progressbar(len(runList),'Running EddyPro')
                for i,_ in enumerate(pool.imap(runEP.Batch,runList)):
                    pb.step()
                for thread in threads:
                    shutil.rmtree(os.getcwd()+f'/temp/{thread.pid}')

                pool.close()
