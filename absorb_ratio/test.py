import pandas as pd
import numpy as np
from pandasquant import logret2algret
from pandasquant.staff import Analyzer
from .ar import calc_industry_ar


def forward_return(close_price: pd.DataFrame, period: str):
    ret = np.log(close_price / close_price.shift(1))
    ret = ret.resample(period).sum()
    ret = ret.shift(-1)
    ret = logret2algret(ret).stack(dropna=False)
    ret.name = period
    return ret

def factor_data(n_component: int = None, window: int = None, period: str = None, filepath: str = None):
    names = pd.read_excel('assets/data/industry.nosync/industry_name.xlsx', index_col=1).drop('行业名称', axis=1)
    names = names.to_dict()['行业代码']

    if filepath is not None:
        factor_data = pd.read_csv(filepath, index_col=0, parse_dates=True)
        factor_data = factor_data.resample(period).last().dropna()
        factor_data = factor_data.rename(columns=names)
        factor_data = factor_data.stack()
        factor_data.name = 'ar'
        factor_data = factor_data.to_frame()
        factor_data['group'] = factor_data.index.get_level_values(1)
        return factor_data
    else:
        if n_component is None or window is None:
            raise ValueError('n_component and window must be given')
        else:
            close_price = pd.read_csv('assets/data/industry.nosync/citics_close.csv', index_col=0, parse_dates=True)
            factor_data = calc_industry_ar(close_price, n_component, window)
            factor_data = factor_data.stack()
            factor_data.name = 'ar'
            factor_data = factor_data.to_frame()
            factor_data['group'] = factor_data.index.get_level_values(1)
            return factor_data

def standarize_data(factor_data: pd.DataFrame, fwd: pd.Series) -> pd.DataFrame:
    data = pd.merge(factor_data, fwd, left_index=True, right_index=True)
    data.index.names = ['date', 'assets']
    return data


if __name__ == "__main__":
    close_price = pd.read_csv('assets/data/industry.nosync/citics_close.csv', index_col=0, parse_dates=True)
    ret = forward_return(close_price, '1M')
    ar = factor_data(filepath='absorb_ratio/result/compcontrib.csv', period='1M')
    data = standarize_data(ar, ret)
    ana = Analyzer(data)
    ana.regression()
    ana.ic()
    ana.holding_profit()
    ana.ic_plot()
    ana.layering()
    ana.layering_plot()
