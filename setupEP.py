# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import sys
import yaml
import time
import runEP
import shutil
import argparse
import importlib
import numpy as np
import pandas as pd
import configparser
import db_root as db
import multiprocessing
from datetime import datetime
from multiprocessing import Pool
from HelperFunctions import progressbar, sub_path

class makeRun():

    def __init__(self,template_file,siteID,name=None,testing=False,Processes = 2,priority = 'normal'):
        self.priority = priority
        self.Processes = Processes
        if name is None:
            self.name = f"{siteID}"
        else:
            self.name = f"{name}_{siteID}"

        self.DeBug = testing
        
        self.ini = {}
        ymls = ['config.yml']
        for y in ymls:
            with open(y) as yml:
                self.ini.update(yaml.safe_load(yml))
        
        self.ini.update(db.config)

        self.siteID = siteID

        # Template file to be filled updated
        self.epRun = configparser.ConfigParser()
        self.epRun.read(template_file)

        # Parameters to update in template
        self.epUpdate = configparser.ConfigParser()
        self.epUpdate.read('ini_files/EP_Dynamic_Updates.ini')
        
        # Parameters to update in template
        self.epDataCols = configparser.ConfigParser()

        class_dict = self.__dict__
        class_dict.update(self.ini['RootDirs'])
        class_dict.update(self.ini['Paths']['Substitutions'])
        
        time_invariant = {}
        for key,path in self.ini['Paths'].items():
            if isinstance(path,str):
                self.ini['Paths'][key] = sub_path(class_dict,path)
                time_invariant[key] = self.ini['Paths'][key]
        self.ini['Paths']['time_invariant'] = time_invariant

        self.inventory = pd.read_csv(self.ini['Paths']['meta_dir']+self.ini['filenames']['file_inventory'],parse_dates=['TIMESTAMP'],index_col='TIMESTAMP')
        self.inventory = self.inventory.loc[((self.inventory['filename'].isna()==False)&(self.inventory['MetaDataFile'].isna()==False))].copy()
    
    def updateTemplate(self,updates):
        for key,val in updates.items():
            for k,v in val.items():
                self.epUpdate[key][k]=str(v)

    def runDates(self,dateRange):
        self.runList = []
        self.epRun['Project']['project_title']=self.name
        # Simple/tidy procedure for testing - replace with more secure process that doesn't overwrite outputs
        self.output_path = sub_path(self.__dict__,self.ini['Paths']['eddypro_output']) 
        if os.path.isdir(self.output_path):
            if len(os.listdir(self.output_path)) >= 0:
                input("Warning: Output directory is not empty.  Move files before running or they will be deleted.  Press any key to continue:")
            shutil.rmtree(self.output_path)
        if ~os.path.isdir(self.output_path):
            os.makedirs(self.output_path)
        batch_path = self.output_path+'EP_Batch_Logs/'
        if ~os.path.isdir(batch_path):
            os.makedirs(batch_path)

        # Setup the runs - first find all unique metadata files in a given time-window
        Range_index = pd.DatetimeIndex(dateRange)
        Subset_Inventory = self.inventory.loc[((self.inventory.index>=Range_index[0])&(self.inventory.index<=Range_index[1]))].copy()
        
        # Find data gaps > 1 day; use as an additional axis to group runs
        Subset_Inventory['ix']=Subset_Inventory.reset_index().index.values
        Subset_Inventory['set'] = Subset_Inventory.index.to_series().diff()
        Subset_Inventory['set'] = (Subset_Inventory['set'].dt.days+Subset_Inventory['set'].dt.seconds/(24*3600)).fillna(1)
        Subset_Inventory.loc[Subset_Inventory['set']<1,'set']=np.nan
        Subset_Inventory.loc[~Subset_Inventory['set'].isna(),'set']=Subset_Inventory.loc[~Subset_Inventory['set'].isna(),'ix']
        Subset_Inventory['set']=Subset_Inventory['set'].ffill()
        Metadata_Files_in_Range = Subset_Inventory.groupby(['MetaDataFile','name_pattern','set']).first().index.values
        # Each unique (based on metadata files) time period within the range index will be split by the number of Processes
        if len(Metadata_Files_in_Range)>1:
            print(f"Splitting into {len(Metadata_Files_in_Range)} batches due to update metadata")
        for i,(Metadata_File,search_pattern,set) in enumerate(Metadata_Files_in_Range):
            sub_subset = Subset_Inventory.loc[((Subset_Inventory['MetaDataFile']==Metadata_File)&(Subset_Inventory['name_pattern']==search_pattern)&(Subset_Inventory['set']==set))]
            Range_index = sub_subset.index
            step = np.floor(Range_index.size/self.Processes)
            if step <= self.Processes:
                step = np.floor(Range_index.size/1)
                nsteps=1
            else:
                nsteps=self.Processes
            for j,ix in enumerate([[int(k*step),int(k*step+step)] for k in range(nsteps)]):
                if j<self.Processes-1:
                    run_ix = Range_index[ix[0]:ix[1]]
                else: # Final thread will play cleanup and get any extra runs
                    run_ix = Range_index[ix[0]:]
                    
                pr_start_date = str(run_ix[0].date())
                pr_start_time = str(run_ix[0].time())[:5]
                pr_end_date = str(run_ix[-1].date())
                pr_end_time = str(run_ix[-1].time())[:5]
                if len(run_ix.year.unique().values)>1:
                    self.year = None
                    self.month = None
                else:
                    self.year = run_ix.year[0]
                    if len(run_ix.month.unique().values)>1:
                        self.month = None
                    else:
                        self.month = str(run_ix.month[0]).zfill(2)

                if sub_subset.loc[((sub_subset.index>=run_ix[0])&(sub_subset.index<=run_ix[-1]))].index.size>0:
            
                    # Copy the metadata to the output location
                    shutil.copy2(self.ini['Paths']['meta_dir']+Metadata_File,batch_path+Metadata_File)
                    
                    # Name the run
                    # start_str = run_ix[0].strftime('%Y-%m-%dT%H%M')
                    # end_str = run_ix[-1].strftime('%Y-%m-%dT%H%M')
                    start_str = run_ix[0].strftime('%Y%m%d')
                    end_str = run_ix[-1].strftime('%Y%m%d')
                    project_id = f'{self.name}_{start_str}_{end_str}_{i}'
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
                        self.runList.append(file_name) 
          
        self.submit()
        self.merge_outputs()
        
    
    def submit(self):
        if len(self.runList) < self.Processes:
            self.Processes = len(self.runList)
        if (__name__ == 'setupEP' or __name__ == '__main__') and self.Processes > 1:
            with Pool(processes=self.Processes) as pool:
                self.Processes = multiprocessing.active_children()
                
                for thread in self.Processes:
                    cwd = os.getcwd()
                    bin = cwd+f'/temp/{thread.pid}/bin/'
                    ini = cwd+f'/temp/{thread.pid}/ini/'

                    shutil.copytree(self.ini['RootDirs']['EddyPro'],bin)

                    batchFile=f'{bin}runEddyPro.bat'.replace('/',"\\")
                    with open(batchFile, 'w') as batch:                        
                        contents = f'cd {bin}'
                        P = self.priority.lower().replace(' ','')
                        # contents+=f'\nSTART cmd /c '+self.ini['filenames']['eddypro_rp']+' ^> processing_log.txt'
                        contents+=f'\nSTART powershell  ".\\'+self.ini['filenames']['eddypro_rp']+' | tee processing_log.txt"'
                        contents+='\nping 127.0.0.1 -n 6 > nul'
                        contents+=f'\nwmic process where name="{self.ini["filenames"]["eddypro_rp"]}" CALL setpriority "{self.priority}"'
                        contents+='\nping 127.0.0.1 -n 6 > nul'
                        contents+='\nEXIT'
                        batch.write(contents)
                    os.mkdir(ini)

                pb = progressbar(len(self.runList),'Running EddyPro')
                for i,_ in enumerate(pool.imap(runEP.Batch,self.runList)):
                    pb.step()
                for thread in self.Processes:
                    shutil.rmtree(os.getcwd()+f'/temp/{thread.pid}')

                pool.close()
        elif len(self.runList)>0:
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
                P = self.priority.lower().replace(' ','')
                # contents+=f'\nSTART cmd /c '+self.ini['filenames']['eddypro_rp']+' ^> processing_log.txt'
                contents+=f'\nSTART powershell  ".\\'+self.ini['filenames']['eddypro_rp']+' | tee processing_log.txt"'
                contents+='\nping 127.0.0.1 -n 6 > nul'
                contents+=f'\nwmic process where name="{self.ini["filenames"]["eddypro_rp"]}" CALL setpriority "{self.priority}"'
                contents+='\nping 127.0.0.1 -n 6 > nul'
                contents+='\nEXIT'
                batch.write(contents)
            os.mkdir(ini)
            for r in self.runList:
                runEP.Batch(r)
            if self.DeBug == False:
                shutil.rmtree(os.getcwd()+f'/temp/{os.getpid()}')

    def merge_outputs(self):
                    
        merge_keys = ['_fluxnet_','_full_output_','_metadata_','_biomet_']
        main_header_col = [0,1,0,0]
        head_length = [1,3,1,2]
        self.all_outputs = {}
        rt = datetime.now().strftime("%Y-%m-%dT%H%M%S")
        for m,h,hl in zip(merge_keys,main_header_col,head_length):
            files = [f for f in os.listdir(self.output_path) if (m in f and f.endswith('.csv'))]
            fullFile = pd.DataFrame()
            for i,f in enumerate(files):
                if i == 0:
                    start = f.split(self.name)[-1].split('_')[1]
                    end = f.split(self.name)[-1].split('_')[2]
                else:
                    start = min([start,f.split(self.name)[-1].split('_')[1]])
                    end = max([end,f.split(self.name)[-1].split('_')[2]])
                df = pd.read_csv(self.output_path+f,header=None,keep_default_na=False)
                while df.loc[h].duplicated().sum()>0:
                    # Eddy pro can potentially output "duplicate" mean values: eg., two columns labelled "air_p_mean"
                    ix = df.loc[h].duplicated()
                    df.loc[h,ix]+='_alt'
                # set the first hl rows as columns
                df.columns=pd.MultiIndex.from_arrays(df.iloc[0:hl].values)
                # delete the first three rows (because they are also the columns)
                df=df.iloc[hl:]
                fullFile = pd.concat([fullFile,df],axis=0,sort=False)
                fullFile.sort_values(by=fullFile.columns[0])
                fullFile = fullFile.loc[fullFile.duplicated()==False].copy()
                os.remove(self.output_path+f)
            if len(files)>0:
                fn = f'{self.output_path}eddypro_{self.name}_{start}_{end}{m}{rt}_adv.csv'
                fullFile.to_csv(fn,index=False)
                self.all_outputs[m.replace('_','')]=fn



# If called from command line ...
if __name__ == '__main__':
    
    # Set to cwd to location of the current script
    Home_Dir = os.path.dirname(sys.argv[0])
    os.chdir(Home_Dir)

    # Parse the arguments
    CLI=argparse.ArgumentParser()
    
    CLI.add_argument(
    "--siteID", 
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
    "--self.Processes", 
    nargs="?",# Use "?" to limit to one argument instead of list of arguments
    type=int,  
    default=4,
    )

    CLI.add_argument(
    "--Priority", 
    nargs="?",# Use "?" to limit to one argument instead of list of arguments
    type=str,  
    default='high self.priority',
    )

    CLI.add_argument(
    "--RunDates", 
    nargs='+', # 1 or more values expected => creates a list
    type=str,
    default=[],
    )

    # parse the command line
    args = CLI.parse_args()


    print(f'Initializing {args.Name} run for {args.siteID} with {args.Processes} processes at {args.Priority} self.priority level')
    
    mR = makeRun(args.Template,args.siteID,args.Processes,args.Priority)

    for start,end in zip(args.RunDates[::2],args.RunDates[1::2]):
        t1 = time.time()
        print(f'Processing date range {start} - {end}')
        print(args.Name)
        self.runDates([start,end],args.Name)
        print(f'Completed in: {np.round(time.time()-t1,4)} seconds')

