import pandas as pd

class EventLog():
    # Logs exceptions
    def __init__(self):
        self.dfLog = pd.DataFrame({
            'Flag': pd.Series(dtype='str')
        })
        self.dfLog.index.name='timestamp'

    def errorLog(self,Issue,Record,TimeStamp):
        Flag = f'{Issue}:{Record}'
        try:
            self.dfLog.loc[TimeStamp,'Flag']+='|'+Flag
        except:
            self.dfLog.loc[TimeStamp,'Flag']=Flag
            pass
    def cleanLog(self,TimeStamp):
        if (self.dfLog.index==TimeStamp).any() == False:
            self.dfLog.loc[TimeStamp,'Flag'] = 'No Issues'