# Code to facilitate pre-processing of GHG files for automated flux recalculations
# Created by Dr. June Skeeter

import os
import sys
from pathlib import Path
import zipfile
import configparser
import numpy as np
import pandas as pd
from pathlib import Path
from io import TextIOWrapper
import xml.etree.ElementTree as ET
import datetime
import importlib

import readLiConfigFiles as rLCF
importlib.reload(rLCF)
import eventLog as eL
importlib.reload(eL)


class Parse():
    def __init__(self,ini):
        self.ini = ini
        self.Vars = configparser.ConfigParser()
        self.Vars.read_file(open(self.ini['templates']['VariableList']))

        if self.ini['templates']['UpdateMetadata'] != '':
            self.UpdateMetadata = pd.read_csv('ini_files/BB_Metadata_Updates.csv',parse_dates={'Start':['TIMESTAMP_Start'],'End':['TIMESTAMP_End']})
            self.UpdateMetadata['End']=self.UpdateMetadata['End'].fillna(pd.to_datetime(datetime.datetime.now()).ceil('H'))
            self.UpdateMetadata.reset_index(inplace=True)
            self.UpdateMetadata.set_index(['Start','End','index'],inplace=True)

        self.Metadata = configparser.ConfigParser()
        self.MetadataTemplate = configparser.ConfigParser()

        self.Header = configparser.ConfigParser()                

        biometData = pd.read_csv(self.ini['Paths']['biomet_data'])
        self.biometTraces = biometData.columns
        
        self.dataValues = {}

        self.EV = eL.EventLog()
        self.getCal = rLCF.read_LI_Config(Log=self.EV)

    def process_file(self,input,Template_File=True,Testing=False):
        self.Template_File = Template_File
        name = input[0]
        self.TimeStamp = input[1]
        self.fullFile = self.ini['Paths']['dpath']+'/'+name
        _, self.file_type = name.rsplit('.',1)
        if self.file_type == 'ghg':
            with zipfile.ZipFile(self.fullFile, 'r') as ghgZip:
                self.whichFiles(ghgZip.namelist())
                # Read the metadata file
                self.Template_File = self.Parse_Metadata(TextIOWrapper(ghgZip.open(self.ghgFiles['metadata']), 'utf-8'),self.Template_File)
                
                if self.Template_File == False:
                    self.readHeader(ghgZip.open(self.ghgFiles['data']))

                self.read_dat(ghgZip.open(self.ghgFiles['data']))

                # Read the calibration config data (only do once per day for expedience)
                if self.ini['Calibration_Info']['read_Cal'] == 'True' and (self.TimeStamp.strftime('%H:%M') == '00:00' or Testing == True):
                    if len(self.ghgFiles['system_config']['xmlFiles'])>0:
                        for i, f in enumerate(self.ghgFiles['system_config']['xmlFiles']):
                            self.getCal.readXML(ET.parse(ghgZip.open(f)),i,self.TimeStamp)
                    elif self.ghgFiles['system_config']['co2app'] != '':
                        # Read the co2app file (and header info) - only useful if there is a 7200
                        self.getCal.readConfig(ghgZip.open(self.ghgFiles['system_config']['co2app']).read().decode("utf-8"),self.TimeStamp)
                    else:
                        self.EV.errorLog('Calibration','Missing',self.TimeStamp)

            self.EV.cleanLog(self.TimeStamp)
        else:
            templateFiles = [path for path in Path(self.ini['Paths']['dpath']).rglob("*.metadata")]
            self.Parse_Metadata(open(templateFiles[-1]),self.Template_File)
            self.readHeader(self.fullFile)
            self.read_dat(self.fullFile)
            self.EV.cleanLog(self.TimeStamp)
            self.Metadata_Filename = os.path.basename(templateFiles[-1])
        return({'TimeStamp':self.TimeStamp,
                'dataValues':self.dataValues.copy(),
                'MetadataFile':self.Metadata_Filename,
                'Update':self.EV.dfLog['Update'].copy(),
                'Flag':self.EV.dfLog['Flag'].copy(),
                'calData':self.getCal.calData.copy(),
                'Updated':self.Template_File})

    def readHeader(self,file):
        self.Variable_Names = {}
        self.Variable_Positions = {}
        if self.file_type == 'ghg':
            self.data_columns = np.array(file.readlines()[int(self.Metadata['FileDescription']['header_rows'])-1].decode('utf-8').split(self.delimiter))
        elif self.file_type == 'dat':
            with open(file,'r') as f:
                head = f.readlines()
                self.data_columns = np.array(head[int(self.Metadata['FileDescription']['header_rows'])-3].replace('"', '').replace('\n','').split(self.delimiter))
                self.data_units = np.array(head[int(self.Metadata['FileDescription']['header_rows'])-2].replace('"', '').replace('\n','').split(self.delimiter))

        # Assumes site is running one 7200 or 7550, not both or multiples.  May need to implement more nuanced process if site has multiple co2 analyzers
        if '72' in self.Metadata['Instruments']['instr_2_model']:
            self.Vars['Project']['col_diag_75']=self.Vars['Project']['col_diag_75'].split(',',1)[1]
        if '75' in self.Metadata['Instruments']['instr_2_model']:
            self.Vars['Project']['col_diag_75']=self.Vars['Project']['col_diag_72'].split(',',1)[1]
        for to_process in ['Sonic','Project','Auxillary']:
            for key,val in self.Vars[to_process].items():
                for rec in val.split(','):
                    if rec in self.data_columns:
                        col_num = np.where(self.data_columns==rec)[0]
                        self.data_columns[col_num] = key
                        self.Variable_Positions[key] = col_num[0]
                        self.Variable_Names[key] = rec
                        break

    def read_dat(self,file):
        # with file as f:
        Data = pd.read_csv(file,delimiter=self.delimiter,skiprows=int(self.Metadata['FileDescription']['header_rows']),header=None)
        Data.columns = self.data_columns
        self.dataValues['n_samples'] = Data.shape[0]
        for key,val in self.Variable_Names.items():
            if 'diag' in key:
                self.getStats(Data,key,'max')
            else:
                self.getStats(Data,key)
            if key == 'sos':
                #  Sonic Virtual Temp, eq 9 pg C-2: https://s.campbellsci.com/documents/ca/manuals/csat3_man.pdf
                Data['t_sonic'] = (Data[key]**2/(1.4*287.04))
                self.getStats(Data,'t_sonic')
        # print(self.TimeStamp)
        # print(self.dataValues['t_sonic'])
        # Data['w_prime']=Data['w']-Data['w'].mean()
        # p_bar=Data['col_air_p'].mean()
        for key,val in self.Vars['Custom'].items():
            self.dataValues[key]=eval(val)
        #     Data[f'{key}_prime'] = Data[key]-Data[key].mean()
        #     Flux = p_bar*(Data['w_prime']*Data[f'{key}_prime']).mean()
        #     print('Raw ',flux,' ',Flux)

        if self.Template_File is False:
            self.Write_EP_Template(Data.columns)              
              
    def Parse_Metadata(self,meta_file,MetadataTemplate_File):
        

        # Look for changes in the metadata file and return new metadata template file for each update
        # Correct metadata where necessary (e.g., undocumented orientation change)
        # See ini_files/Metadata_Instructions.ini for metadata varialbes bing 

        self.Metadata.read_file(meta_file)

        if self.ini['templates']['UpdateMetadata'] != '':
            Start = self.UpdateMetadata.index.get_level_values('Start')
            End = self.UpdateMetadata.index.get_level_values('End')
            To_update = self.UpdateMetadata.loc[((Start<=self.TimeStamp)&(End>=self.TimeStamp))]
            cid = ''
            for ix,row in To_update.iterrows():
                columns = row.dropna().index
                values = row.dropna()
                for col,cel in zip(columns,values):
                    self.Metadata[col.split(' ')[0]][col.split(' ')[1]]=str(cel)
        
        templateList = [path.__str__() for path in Path(self.ini['Paths']['meta_dir']).rglob(f"*_{self.Metadata['Station']['logger_id']}.metadata")]
        templateList.sort()
        templateFiles=[templateList[i] for i in range(len(templateList)) if os.path.basename(templateList[i]) < meta_file.name]
        if len(templateFiles)<1:
            MetadataTemplate_File = False
        if MetadataTemplate_File is True:
            self.MetadataTemplate.read_file(open(templateFiles[-1]))
            
            for section,values in self.MetadataTemplate.items():
                for key in values.keys():
                    if key in self.ini['Monitor']['dynamic_values'].split(','):
                        self.dataValues[key] = float(self.Metadata[section][key])
                    elif key in self.ini['Calculate'].keys():
                        self.dataValues[key] = eval(self.ini['Calculate'][key])
                    elif key in self.ini['Monitor']['orientation'].split(','):
                        # Overwrite any values from the correction
                        if self.Metadata[section][key]!=self.MetadataTemplate[section][key]:
                            self.EV.updateLog(f'Orientation {key}',self.Metadata[section][key],self.TimeStamp)
                            MetadataTemplate_File = False
                    elif key not in self.ini['Overwrite'].keys():
                        if self.Metadata[section][key]!=self.MetadataTemplate[section][key]:
                            self.EV.updateLog(f'Other {key}',self.Metadata[section][key],self.TimeStamp)
                            MetadataTemplate_File = False

        if MetadataTemplate_File is False:    
            self.MetadataTemplate.read_dict(self.Metadata)
            filename = f"{self.TimeStamp.strftime('%Y-%m-%dT%H%M%S')}_{self.Metadata['Station']['logger_id']}.metadata"
            self.Metadata_Filename = filename
            for section,values in self.MetadataTemplate.items():
                for key in values.keys():
                    if key in self.ini['Overwrite'].keys():
                        self.MetadataTemplate[section][key] = self.ini['Overwrite'][key]
            with open(f"{self.ini['Paths']['meta_dir']}{filename}", 'w') as template:
                template.write(';GHG_METADATA\n')
                self.MetadataTemplate.write(template,space_around_delimiters=False)
        else:
            self.Metadata_Filename = os.path.basename(templateFiles[-1])
        self.delimiter = self.ini['delimiter_key'][self.Metadata['FileDescription']['separator']].encode('ascii','ignore').decode('unicode_escape')

        return(MetadataTemplate_File)

    def Write_EP_Template(self,cols):
        self.EddyProTemplate = configparser.ConfigParser()
        for v in ['Project','RawProcess_BiometMeasurements']:
            self.EddyProTemplate.add_section(v)
        # print(cols)
        for key,value in self.Vars['Project'].items():
            # for var in value.split(','):
            col_num = np.where(cols==key)[0]
            if len(col_num) == 0:
                channel = 0
            # elif (('72' in self.Metadata['Instruments']['instr_2_model']) or ('72' in self.Metadata['Instruments']['instr_3_model'])) and key == 'col_diag_75':
            #     channel = 0
            #     print(self.Metadata['Instruments']['instr_2_model'],key)
            # elif (('75' in self.Metadata['Instruments']['instr_2_model']) or ('75' in self.Metadata['Instruments']['instr_3_model'])) and key == 'col_diag_72':
            #     channel = 0
            #     print(self.Metadata['Instruments']['instr_2_model'],key)
            elif len(col_num)>1:
                # haven't encountered yet - just a catch to crash the program in case it happens
                warning = f'Warning! Duplicate Channel {key} in Column Headers'
                sys.exit(warning)
            else:
                channel = int(col_num[0])
                # break
            self.EddyProTemplate['Project'][key] = str(channel)
            # print(key,self.EddyProTemplate['Project'][key])

        for key,value in self.Vars['RawProcess_BiometMeasurements'].items():
            match = [i for i in range(len(self.biometTraces)) if self.biometTraces[i] == value]
            if len(match)==1:
                bm_ix = match[0]+1
                self.EddyProTemplate['RawProcess_BiometMeasurements'][key] = str(bm_ix)

        with open(f"{self.ini['Paths']['meta_dir']}{self.TimeStamp.strftime('%Y-%m-%dT%H%M%S')}_{self.Metadata['Station']['logger_id']}.eddypro", 'w') as eddypro:
            eddypro.write(';EDDYPRO_PROCESSING\n')
            self.EddyProTemplate.write(eddypro,space_around_delimiters=False)


    def whichFiles(self,files):
        # Get inventory of files in the zipped .ghg collection
        # Ignoring biomet data, can expand to include later if needed - likely not necessary
        self.ghgFiles = {'data':'',
                        'metadata':'',
                        'status':'',
                        'system_config':{
                            'co2app':'',
                            'xmlFiles':[]
                        }}
        for f in files:
            if 'biomet' in f:
                pass
            elif '.metadata' in f:
                self.ghgFiles['metadata']=f
            elif '.data' in f:
                self.ghgFiles['data']=f
            elif '.status' in f:
                self.ghgFiles['status']=f
            elif 'system_config' in f:
                if 'co2app' in f:
                    self.ghgFiles['system_config']['co2app']=f
                elif '.xml' in f and 'factory' not in f:
                    self.ghgFiles['system_config']['xmlFiles'].append(f)

    def getStats(self,tbl,rec,stat='mean'):
        try:
            if stat == 'mean':
                self.dataValues[rec] = tbl[rec].mean()
            elif stat == 'max':
                self.dataValues[rec] = tbl[rec].max()
        except:
            self.EV.errorLog('Missing Data Value',rec,self.TimeStamp)
            pass
