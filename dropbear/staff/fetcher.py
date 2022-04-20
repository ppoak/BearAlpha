import pandas as pd
from ..tools import *


def read_csv(path, index_col=None, parse_dates=True, panel=False, **kwargs):
    """Read csv file and return a dataframe or a PanelFrame."""
    if panel:
        if index_col is None:
            index_col = [0, 1]
        return PanelFrame(dataframe=pd.read_csv(path, index_col=index_col, parse_dates=parse_dates, **kwargs))
    else:
        if index_col is None:
            index_col = 0
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
        if index_col is None:
            index_col = 0
        return pd.read_excel(path, index_col=index_col, parse_dates=parse_dates, **kwargs)
            
                