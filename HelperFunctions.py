import sys
import os
import numpy as np
import pandas as pd

def queryBiometDatabase(siteID,outputPath,biometPath,database,dateRange):
    # UBC Micromet users can use this to generate Biomet and daynamicMetadata on the fly
    wd = os.path.dirname(os.path.realpath(__file__))
    sys.path.insert(1,os.path.abspath(biometPath+'/Python'))
    import csvFromBinary as cfb    
    sys.path.remove(os.path.abspath(biometPath+'/Python'))
    createAuxilaryData = os.path.abspath(wd+'/config_files/BiometDataFileTemplate.yml')
    auxilaryDpaths=cfb.makeCSV(
        siteID=siteID,
        outputPath=outputPath,
        database=database,
        tasks=createAuxilaryData,
        stage='Second',
        dateRange=dateRange,
        nameTimeStamp=False)
    return(auxilaryDpaths)

def dumpToBiometDatabase(siteID,biometPath,database,inputFile,metaData,stage,tag=''):
    sys.path.insert(1,os.path.abspath(biometPath+'/Python'))
    import binaryFromText as bft
    sys.path.remove(os.path.abspath(biometPath+'/Python'))
    if tag != '': stage = f"{stage}_{tag}"
    print(inputFile,metaData)
    bft.writeTraces(siteID,
                    inputFile,
                    metaData,
                    database=database,
                    excludeCols=['bad*','x_??%','none*'],
                    mode='repfill',
                    stage=stage,
                    verbose=False)

def instrumentSeparation(northOffset,u,v):
    # for CSAT3
    # northOffset is degrees from North
    # u is distance along main axis of sonic (positive=behind)
    # v is distance perpendicular to main axis (positive = right side as viewed from front)

    # Convert to "math wind direction"
    mwd = 270-northOffset
    rad = mwd*np.pi/180
    x = np.sin(rad)*u+np.sin(rad)*v
    y = np.cos(rad)*u+np.cos(rad)*v
    return(x,y)

# substitute keys in a path with corresponding path_strings from a class object 
# remove from the path if it doesn't exist

def sub_path(class_dict,path_string):

    for i in range(3):
        for sub in class_dict.keys():
            try:
                path_string = path_string.replace(f'**{sub}**',f'{class_dict[sub]}')#.replace('None','')
            except:
                pass
                path_string = path_string.replace(f'**{sub}**','')
    
    path_string = path_string.replace('//','/')
    path_string = path_string.replace('\\\\','\\')

    return(path_string)

## Progress bar to update status of a run
class progressbar():

    def __init__(self,items,prefix,size=60,out=sys.stdout):
        self.items = items
        self.out = out
        self.i = 0
        self.prefix=prefix
        self.size=size
        self.show(0)

    def show(self,j):
        if self.items > 0:
            x = int(self.size*j/self.items)
            print(f"{self.prefix}[{u'█'*x}{('.'*(self.size-x))}] {j}/{self.items}", end='\r', file=self.out, flush=True)

    def step(self,step_size=1):
        self.i+=step_size
        self.show(self.i)

    def close(self):
        print('\n')


# Logs exceptions and configuration changes that come up during pre-processing
class EventLog():
    def __init__(self,classes=['Flag']):
        self.Log={}
        for c in classes:
            self.Log[c] = ''

    def updateLog(self,Record,Value,Key='Flag'):
        Update = f'{Record}:{Value}'
        self.Log[Key]=(self.Log[Key]+'|'+Update).lstrip('|')