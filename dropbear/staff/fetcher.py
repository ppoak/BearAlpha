import pandas as pd
from ..tools import *


def read_csv(path, index_col=None, parse_dates=True, panel=False, **kwargs):
    """Read csv file and return a dataframe or a PanelFrame."""
    if panel:
        if index_col is None:
            index_col = [0, 1]
        return PanelFrame(dataframe=pd.read_csv(path, index_col=index_col, parse_dates=parse_dates, **kwargs))
    else:
        return pd.read_csv(path, index_col=index_col, parse_dates=parse_dates, **kwargs)

def read_excel(path, index_col=None, parse_dates=True, 
    indicators=False, assets=False, datetimes=False, **kwargs):
    '''Read excel file and return a dataframe of a panelframe'''
    if indicators or assets or datetimes:
        if index_col is None:
            index_col = 0
        pf = pd.read_excel(path, index_col=index_col, parse_dates=parse_dates, sheet_name=None, **kwargs)
        if indicators and not assets and not datetimes:
            return PanelFrame(indicators=pf)
        elif not indicators and assets and not datetimes:
            return PanelFrame(assets=pf)
        elif not indicators and not assets and datetimes:
            return PanelFrame(datetimes=pf)
        else:
            raise ValueError('at most one of indicators, assets, datetimes can be True')

    if isinstance(index_col, list):
        pf = pd.read_excel(path, index_col=index_col, parse_dates=parse_dates, **kwargs)
        return PanelFrame(dataframe=pf)
    
    else:
        return pd.read_excel(path, index_col=index_col, parse_dates=parse_dates, **kwargs)
            
@pd.api.extensions.register_dataframe_accessor("fetcher")
class Fetcher(Worker):
    def __init__(self, dataframe: pd.DataFrame, **kwargs):
        super().__init__(dataframe)
    
    def to_multisheet_excel(self, path, **kwargs):
        if self.type_ == Worker.PANEL:
            with pd.ExcelWriter(path) as writer:
                for column in self.dataframe.columns:
                    self.dataframe[column].unstack(level=1).to_excel(writer, sheet_name=str(column), **kwargs)
        
        else:
            self.dataframe.to_excel(path, **kwargs)


if __name__ == '__main__':
    import numpy as np

    data = pd.DataFrame(np.random.rand(100, 20), 
        index=pd.MultiIndex.from_product([pd.date_range('20210101', periods=5), range(20)]))
    data.fetcher.to_multisheet_excel('test.xlsx')
