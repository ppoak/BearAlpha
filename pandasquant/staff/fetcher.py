import pandas as pd
from ..tools import *        
@pd.api.extensions.register_dataframe_accessor("filer")
class Filer(Worker):
    
    def to_multisheet_excel(self, path, **kwargs):
        if self.type_ == Worker.PANEL:
            with pd.ExcelWriter(path) as writer:
                for column in self.dataframe.columns:
                    self.dataframe[column].unstack(level=1).to_excel(writer, sheet_name=str(column), **kwargs)
        
        else:
            self.dataframe.to_excel(path, **kwargs)

    @staticmethod
    def read_excel(path, indicators=False, assets=False, datetimes=False, **kwargs):
        '''A dummy function of pd.read_csv, which provide multi sheet reading function'''
        if indicators or assets or datetimes:
            sheets_dict = pd.read_excel(path, sheet_name=None)
            datas = []
            
            if indicators and not assets and not datetimes:
                for indicator, data in sheets_dict.items():
                    data = data.stack()
                    data.name = indicator
                    datas.append(data)

            elif not indicators and assets and not datetimes:
                for asset, data in sheets_dict.items():
                    data.index = pd.MultiIndex.from_product([data.index, [asset]])
                    datas.append(data)

            elif not indicators and not assets and datetimes:
                for datetime, data in sheets_dict.items():
                    data.index = pd.MultiIndex.from_product([[datetime], data.index])
                    datas.append(data)
            
            else:
                raise ValueError('at most one of indicators, assets, datetimes can be True')

        else:
            return pd.read_excel(path, **kwargs)

class Databaser(object):
    pass


if __name__ == '__main__':
    import numpy as np

    data = pd.DataFrame(np.random.rand(100, 20), 
        index=pd.MultiIndex.from_product([pd.date_range('20210101', periods=5), range(20)]))
    data.fetcher.to_multisheet_excel('test.xlsx')
    
