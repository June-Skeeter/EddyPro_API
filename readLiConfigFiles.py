import re
import json
import numpy as np
import configparser
import eventLog as el
import pandas as pd


def getTime(Time,fmt = 'epoch'):
    try:
        if fmt == 'epoch':
            return(pd.to_datetime(float(Time),unit='s'))
        elif fmt == 'string':
            return(pd.to_datetime(Time))
    except:
        return(pd.to_datetime('NaT'))

class read_LI_Config():
    # Parse the system_config\co2app.conf file from the 7200 to get calibration settings
    # Only an option for co2/h2o the ch4 calibration values are only saved on the LI-7000
    # Unless dealing with xml format - the all values are saved
    # Only available for 7500 so far?

    def __init__(self,ini_file='ini_files/ReadLiConfigFiles.ini',Log=None):
        if Log == None:
            self.EV = el.EventLog()
        else:
            self.EV = Log
            
        self.calData = pd.DataFrame()

        ini = configparser.ConfigParser()
        if type(ini_file) != type(ini):        
            ini.read(ini_file)
        else:
            ini = ini_file
        self.ini_CO2APP=ini['CO2APP']
        self.ini_XML=ini['XML']

    def readConfig(self,file,TimeStamp):
        self.parsedConfig = {}
        splits = self.ini_CO2APP['calibrate'].split(',')
        self.parse(file.split(splits[0])[-1].split(splits[1])[0],'calibrate',TimeStamp)
        i = 0
        for key,val in self.parsedConfig['calibrate'].items():
            if val['date']!='4Cal' and key in self.ini_CO2APP[f'calibrate_map'].split(','):
                val['date'] = getTime(val['date'],fmt='string')
                if 'Zero' in key:
                    val['target']=0
                    val['tdensity']=np.nan
                val['Parameter']=key

                self.calData = pd.concat([self.calData,pd.DataFrame(index=[0],data=val)])

    def parse(self,string,key,TimeStamp,pnt=False):
        all = re.findall(r'\((.+?)\)',string)
        formatted =''
        for i,v in enumerate(all):
            
            if '(' in v:
                tags = v.split('(')[:-1]
                for t in tags:
                    formatted += '\n['+t.replace(' ','')+']\n'
                formatted += v.split('(')[-1].replace(' ','=',1)
            else:
                if ' ' not in v:
                    formatted += '\n'+v+'='
                else:
                    formatted += '\n'+v.replace(' ','=',1)
        if pnt == True:
            print(formatted)
        try:
            config = configparser.ConfigParser()
            config.read_string(formatted)
            self.parsedConfig[key]={section: dict(config[section]) for section in config.sections()}
        except:
            self.EV.errorLog('Calibration',f'Unable to Parse {key} from .conf',TimeStamp)

    def readXML(self,file,i,TimeStamp):
        with open(self.ini_XML['calibrate_map']) as json_file:
            self.Calibrate_map = json.load(json_file)
        if i == 0:
            self.parsedConfig = {}
        root = file.getroot()[0]
        self.parsedConfig[root.tag] = {}

        for child in root:
            if child.tag == self.ini_XML['calibrate']:
                for val in child:
                    self.parsedConfig[root.tag][val.tag] = val.text
        for parameter,values in self.Calibrate_map[root.tag].items():
            values_copy = values.copy()
            for key,val in values.items():
                rep = self.parsedConfig[root.tag][val]
                if key == 'date':
                    if root.tag == 'li7500ds':fmt = 'epoch'
                    else: fmt = 'string'
                    rep = getTime(rep,fmt=fmt)
                if 'Zero' in parameter:
                    values_copy['target']=0
                values_copy[key]=rep
            if pd.isnull(values_copy['date']) == False:
                values_copy['Parameter']=parameter
                self.calData = pd.concat([self.calData,pd.DataFrame(index=[TimeStamp],data=values_copy)])
        