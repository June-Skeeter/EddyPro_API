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
        self.inventory = self.inventory.loc[self.inventory['MetaDataFile']!='-']

    def sub(self,val):
        v = val.replace('SITE',self.Site)
        if hasattr(self, 'Year'):
            v = v.replace('YEAR',str(self.Year))
        if hasattr(self, 'dateStr'):
            v = v.replace('DATE',str(self.dateStr))
        return(v)
    
    def runDates(self,dateRange,name,threads = 2):
        runList = []
        Dates = (pd.date_range(dateRange[0],dateRange[1]))
        for runDate in Dates:
            self.dateStr = str(runDate.date())
            self.Year = runDate.year
            Metadata = self.inventory.loc[self.inventory.index.date==runDate.date(),'MetaDataFile'].unique()
            EddyProColumnUpdate = [f.replace('.metadata','.eddypro') for f in Metadata]
            for md,epcu in zip(Metadata,EddyProColumnUpdate):
                self.epDataCols.read(self.ini['Paths']['meta_dir']+epcu)
                for section in self.epDataCols.keys(): 
                    for key,value in self.epDataCols[section].items():
                        self.epRun[section][key]=value
            for section in self.epUpdate.keys():
                print(section)
                for key,value in self.epUpdate[section].items():
                    # print(key,eval(value))
                    self.epRun[section][key]=eval(value)


        #     self.column_numbers = pd.read_csv(self.sub(self.ini['Paths']['metadata']+self.ini['filenames']['variable_columns']),parse_dates={'timestamp':['date','time']},index_col=['timestamp'])

        #     for section in self.epUpdate.keys():
        #         for key,value in self.epUpdate[section].items():
        #             if key != 'col_numbers':
        #                 self.epRun[section][key]=eval(value)
        #             else:
        #                 columns=self.epUpdate[section][key].split(',')
        #                 for col in columns:
        #                     if col in self.column_numbers.columns:
        #                         self.epRun[section][col]=str(self.column_numbers.loc[self.column_numbers.index.date==runDate.date(),col].values[0])
        #                     else:
        #                         self.epRun[section][col] = '0'

            try:
                os.mkdir(self.sub(self.ini['Paths']['eddypro_output']))
            except:
                pass
            with open(self.epRun['Project']['file_name'], 'w') as eddypro:
                eddypro.write(';EDDYPRO_PROCESSING\n')
                self.epRun.write(eddypro,space_around_delimiters=False)
            runList.append(self.epRun['Project']['file_name'])    

        # if (__name__ == 'setupEP' or __name__ == '__main__') :
        #     with Pool(processes=threads) as pool:
        #         threads = multiprocessing.active_children()
                
        #         for thread in threads:
        #             cwd = os.getcwd()
        #             bin = cwd+f'/temp/{thread.pid}/bin/'
        #             ini = cwd+f'/temp/{thread.pid}/ini/'

        #             shutil.copytree(self.ini['Paths']['eddypro_installation'],bin)

        #             batchFile=f'{bin}runEddyPro.bat'.replace('/',"\\")
        #             with open(batchFile, 'w') as batch:
        #                 contents = f'cd {bin}'
        #                 contents+='\n'+self.ini['filenames']['eddypro_rp']
        #                 contents+='\n'+self.ini['filenames']['eddypro_fcc']
        #                 batch.write(contents)
        #             os.mkdir(ini)

        #         pb = progressbar(len(runList),'Running EddyPro')
        #         for i,_ in enumerate(pool.imap(runEP.Batch,runList)):
        #             pb.step()
        #         for thread in threads:
        #             shutil.rmtree(os.getcwd()+f'/temp/{thread.pid}')

        #         pool.close()
