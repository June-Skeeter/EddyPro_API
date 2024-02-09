import sys
import pandas as pd


# substitute keys in a path with corresponding path_strings from a class object 
# remove from the path if it doesn't exist

def sub_path(class_object,path_string):

    
    if hasattr(class_object, 'Root'):
        path_string = path_string.replace('ROOT_PATH',str(class_object.Root))
    else:
        path_string = path_string.replace('ROOT_PATH','')

    if hasattr(class_object, 'Year'):
        path_string = path_string.replace('YEAR',str(class_object.Year))
    else:
        path_string = path_string.replace('YEAR','')
    
    if hasattr(class_object, 'Month'):
        path_string = path_string.replace('MONTH',str(class_object.Month))
    else:
        path_string = path_string.replace('MONTH','')
    
    if hasattr(class_object, 'SiteID'):
        path_string = path_string.replace('SITEID',str(class_object.SiteID))
    else:
        path_string = path_string.replace('SITEID','')
    
    if hasattr(class_object, 'dateStr'):
        path_string = path_string.replace('DATE',str(class_object.dateStr))
    else:
        path_string = path_string.replace('DATE','')
    
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


# Logs exceptions and configuration changes that come up during pre-processing
class EventLog():
    def __init__(self):
        self.dfLog = pd.DataFrame({
            'Flag': pd.Series(dtype='str'),
            'Update': pd.Series(dtype='str')
        })
        self.dfLog.index.name='timestamp'

    def errorLog(self,Issue,Record,TimeStamp):
        Flag = f'{Issue}:{Record}'
        try:
            self.dfLog.loc[TimeStamp,'Flag']+='|'+Flag
        except:
            self.dfLog.loc[TimeStamp,'Flag']=Flag
            pass

    def updateLog(self,Record,Value,TimeStamp):
        Note = f'{Record}:{Value}'
        try:
            self.dfLog.loc[TimeStamp,'Update']+='|'+Note
        except:
            self.dfLog.loc[TimeStamp,'Update']=Note
            pass

    def cleanLog(self,TimeStamp):
        if (self.dfLog.index==TimeStamp).any() == False:
            self.dfLog.loc[TimeStamp,'Flag'] = '-'