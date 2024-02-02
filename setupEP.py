# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import sys
import runEP
import shutil
import argparse
import configparser
import numpy as np
import pandas as pd
import multiprocessing
from subPath import sub_path
from multiprocessing import Pool
from datetime import datetime

import importlib
importlib.reload(runEP)

from HelperFunctions import progressbar

class makeRun():

    def __init__(self,template_file,SiteID,testing=False):

        self.DeBug = testing
        
        inis = ['configuration.ini']
        ini_file = ['ini_files/'+ini for ini in inis]
        self.ini = configparser.ConfigParser()
        self.ini.read(ini_file)
        
        self.SiteID = SiteID

        # Template file to be filled updated
        self.epRun = configparser.ConfigParser()
        self.epRun.read(template_file)

        # Parameters to update in template
        self.epUpdate = configparser.ConfigParser()
        self.epUpdate.read('ini_files/EP_Dynamic_Updates.ini')
        
        # Parameters to update in template
        self.epDataCols = configparser.ConfigParser()

        self.ini['Paths']['meta_dir'] = sub_path(self,self.ini['Paths']['metadata'])

        self.inventory = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['file_inventory'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
        self.inventory['MetaDataFile'] = self.inventory['MetaDataFile'].fillna('-')
        self.inventory = self.inventory.loc[self.inventory['MetaDataFile']!='-']

    # def sub(self,val):
    #     v = val.replace('SiteID',self.SiteID)
    #     if hasattr(self, 'Year'):
    #         v = v.replace('YEAR',str(self.Year))
    #     if hasattr(self, 'dateStr'):
    #         v = v.replace('DATE',str(self.dateStr))
    #     return(v)
    
    def runDates(self,dateRange,name,Processes = 2,priority = 'normal'):
        runList = []
        self.epRun['Project']['project_title']=name
        # Simple/tidy procedure for testing - replace with more secure process that doesn't overwrite outputs
        output_path = sub_path(self,self.ini['Paths']['eddypro_output']) 
        if os.path.isdir(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path)
        batch_path = output_path+'EP_Runs/'
        if os.path.isdir(batch_path):
            shutil.rmtree(batch_path)
        os.makedirs(batch_path)

        # Setup the runs - first find all unique metadata files in a given time-window
        Range_index = pd.DatetimeIndex(dateRange)
        Subset_Inventory = self.inventory.loc[((self.inventory.index>=Range_index[0])&(self.inventory.index<=Range_index[1]))].copy()
        Metadata_Files_in_Range = Subset_Inventory['MetaDataFile'].unique()

        # Each unique (based on metadata files) time period within the range index will be split by the number of Processes
        if len(Metadata_Files_in_Range)>1:
            print(f"Splitting into {len(Metadata_Files_in_Range)} batches due to update metadata")
        for Metadata_File in Metadata_Files_in_Range:
            Range_index = Subset_Inventory.loc[Subset_Inventory['MetaDataFile']==Metadata_File].index
            step = np.floor(Range_index.size/Processes)
            if step <= Processes:
                print(f"Insufficient number of files in batch for multiprocessing")
                Processes = 1
                step = np.floor(Range_index.size/Processes)
            for i,ix in enumerate([[int(i*step),int(i*step+step)] for i in range(Processes)]):
                if i<Processes-1:
                    run_ix = Range_index[ix[0]:ix[1]]
                else: # Final thread will play cleanup and get any extra runs
                    run_ix = Range_index[ix[0]:]
                    
                pr_start_date = str(run_ix[0].date())
                pr_start_time = str(run_ix[0].time())[:5]
                pr_end_date = str(run_ix[-1].date())
                pr_end_time = str(run_ix[-1].time())[:5]
                self.Year = run_ix[-1].year
            
                # Copy the metadata to the output location
                shutil.copy2(self.ini['Paths']['meta_dir']+Metadata_File,batch_path+Metadata_File)
                
                # Name the run
                start_str = run_ix[0].strftime('%Y-%m-%dT%H%M00')
                end_str = run_ix[-1].strftime('%Y-%m-%dT%H%M00')
                project_id = f'{name}_{start_str}_{end_str}'
                file_name = f'{batch_path}{project_id}.eddypro'

                # Grab the Eddypro input template from preprocessing associated with the metadata file
                EddyProColumnUpdate = Metadata_File.replace('.metadata','.eddypro')
                self.epDataCols.read(self.ini['Paths']['meta_dir']+EddyProColumnUpdate)
                # Dump the template values in to the run file
                for section in self.epDataCols.keys(): 
                    for key,value in self.epDataCols[section].items():
                        self.epRun[section][key]=value

                # Dump custom values using eval statement (see ini_files/EP_Dynamic_Updates.ini)
                for section in self.epUpdate.keys():
                    for key,value in self.epUpdate[section].items():
                        self.epRun[section][key]=eval(value)

                # Save the run and append to the list of runs
                with open(file_name, 'w') as eddypro:
                    eddypro.write(';EDDYPRO_PROCESSING\n')
                    self.epRun.write(eddypro,space_around_delimiters=False)
                    runList.append(file_name)   

        if (__name__ == 'setupEP' or __name__ == '__main__') and Processes > 1 :
            with Pool(processes=Processes) as pool:
                Processes = multiprocessing.active_children()
                
                for thread in Processes:
                    cwd = os.getcwd()
                    bin = cwd+f'/temp/{thread.pid}/bin/'
                    ini = cwd+f'/temp/{thread.pid}/ini/'

                    shutil.copytree(self.ini['Paths']['eddypro_installation'],bin)

                    batchFile=f'{bin}runEddyPro.bat'.replace('/',"\\")
                    with open(batchFile, 'w') as batch:
                        # contents = f'cd {bin}'
                        # P = priority.upper().replace(' ','')
                        # contents+=f'\nSTART /{P} '+self.ini['filenames']['eddypro_rp']
                        # contents+='\n'+f'WMIC process where name="{self.ini["filenames"]["eddypro_rp"]}" CALL setpriority "{priority}"'
                        # contents+='\nSTART '+self.ini['filenames']['eddypro_fcc']
                        # contents+='\nEXIT'
                        # batch.write(contents)
                        
                        contents = f'cd {bin}'
                        P = priority.lower().replace(' ','')
                        contents+=f'\nSTART cmd /c '+self.ini['filenames']['eddypro_rp']+' ^> processing_log.txt'
                        contents+='\nping 127.0.0.1 -n 6 > nul'
                        contents+=f'\nwmic process where name="{self.ini["filenames"]["eddypro_rp"]}" CALL setpriority "{priority}"'
                        contents+='\nping 127.0.0.1 -n 6 > nul'
                        contents+='\nEXIT'
                        batch.write(contents)
                    os.mkdir(ini)

                pb = progressbar(len(runList),'Running EddyPro')
                for i,_ in enumerate(pool.imap(runEP.Batch,runList)):
                    pb.step()
                for thread in Processes:
                    shutil.rmtree(os.getcwd()+f'/temp/{thread.pid}')

                pool.close()
        else:
            cwd = os.getcwd()
            bin = cwd+f'/temp/{os.getpid()}/bin/'
            ini = cwd+f'/temp/{os.getpid()}/ini/'
            if self.DeBug == True:
                try:
                    shutil.rmtree(os.getcwd()+f'/temp/{os.getpid()}')
                except:
                    pass
            shutil.copytree(self.ini['Paths']['eddypro_installation'],bin)
            batchFile=f'{bin}runEddyPro.bat'.replace('/',"\\")
            with open(batchFile, 'w') as batch:
                contents = f'cd {bin}'
                P = priority.lower().replace(' ','')
                contents+=f'\nSTART cmd /c '+self.ini['filenames']['eddypro_rp']+' ^> processing_log.txt'
                contents+='\nping 127.0.0.1 -n 6 > nul'
                contents+=f'\nwmic process where name="{self.ini["filenames"]["eddypro_rp"]}" CALL setpriority "{priority}"'
                contents+='\nping 127.0.0.1 -n 6 > nul'
                contents+='\nEXIT'
                batch.write(contents)
            os.mkdir(ini)
            for r in runList:
                runEP.Batch(r)
            if self.DeBug == False:
                shutil.rmtree(os.getcwd()+f'/temp/{os.getpid()}')
            


# If called from command line ...
if __name__ == '__main__':
    
    # Set to cwd to location of the current script
    Home_Dir = os.path.dirname(sys.argv[0])
    os.chdir(Home_Dir)

    # Parse the arguments
    CLI=argparse.ArgumentParser()
    
    CLI.add_argument(
    "--SiteID", 
    nargs="?",# Use "?" to limit to one argument instead of list of arguments
    type=str,
    default='BB',
    )

    CLI.add_argument(
    "--Template", 
    nargs="?",# Use "?" to limit to one argument instead of list of arguments
    type=str,
    default='ep_Templates/LabStandard_Advanced.eddypro',
    )
    
    CLI.add_argument(
    "--Name", 
    nargs='+', # 1 or more values expected => creates a list
    type=str,
    default='EddyPro_API_Run',
    )
    
    CLI.add_argument(
    "--Processes", 
    nargs="?",# Use "?" to limit to one argument instead of list of arguments
    type=int,  
    default=4,
    )

    CLI.add_argument(
    "--Priority", 
    nargs="?",# Use "?" to limit to one argument instead of list of arguments
    type=str,  
    default='high',
    )

    CLI.add_argument(
    "--RunDates", 
    nargs='+', # 1 or more values expected => creates a list
    type=str,
    default=[],
    )

    # parse the command line
    args = CLI.parse_args()


    print(f'Initializing {args.Name} run for {args.SiteID} with {args.Processes} processes at {args.Priority} priority level')
    
    mR = makeRun(args.Template,args.SiteID)

    for start,end in zip(args.RunDates[::2],args.RunDates[1::2]):
        mR.runDates([start,end],args.Name,args.Processes,args.Priority)
        print(f'Processing date range {start} - {end}')

