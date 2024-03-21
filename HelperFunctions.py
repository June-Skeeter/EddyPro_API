import sys
import numpy as np
import pandas as pd


# substitute keys in a path with corresponding path_strings from a class object 
# remove from the path if it doesn't exist

def sub_path(class_object,path_string):

    sub_patterns = ['read_data_dir','write_data_dir','year','month','siteID','dateStr','name']
    for sub in sub_patterns:
        if hasattr(class_object, sub):
            path_string = path_string.replace(f'**{sub}**',str(class_object.__getattribute__(sub))).replace('None','')
        else:
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