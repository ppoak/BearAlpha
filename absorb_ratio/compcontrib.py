import math
import pandas as pd
import numpy as np
from rich.progress import track

def calc_component_contribution(close_price: pd.DataFrame, n_component: int, window: int):
    ret = (close_price - close_price.shift(1)) / close_price.shift(1)
    # ret = ret.ewm(halflife=20).mean()
    ind_list_all =  [
        'zx_petro','zx_coal','zx_metals','zx_power','zx_steel','zx_chemicals',
        'zx_construct_eng','zx_construct_mat','zx_light_man','zx_machinery',
        'zx_electr_equip','zx_defense','zx_automobiles','zx_retail','zx_hotels_lei',
        'zx_household_dur','zx_textile','zx_medical','zx_food_bev','zx_agriculture',
        'zx_banks','zx_non_bank_fin','zx_real_estate','zx_transportation','zx_electronic_comp',
        'zx_communication','zx_computers','zx_media'
        ]
    comp_contrib = pd.DataFrame(columns=ind_list_all, dtype='float32')
    for idx in track(range(window, ret.shape[0])):
        date: pd.Timestamp = ret.index[idx]
        sample = ret.iloc[idx - window:idx]
        sample_demean = sample - sample.mean(axis=0)
        sample_demean = sample_demean.dropna(how='all')
        industry_info = pd.read_csv(f"assets/data/stock.nosync/status/df_stock_status_{date.strftime('%Y-%m-%d')}.csv", index_col=3).iloc[:, [3, 4] + list(range(22, 50))]
        industry_info = industry_info.loc[(industry_info['is_ST'] == 0) & (industry_info['is_new_stock'] == 0)]
        industry_info = industry_info.drop(['is_ST', 'is_new_stock'], axis=1)
        for ind in industry_info.columns:
            stocks = industry_info.index[industry_info[ind].astype('bool')]
            stocks = ret.columns.intersection(stocks)
            if len(stocks) < n_component:
                print(f'[{date.date()}] {ind} has less than {n_component} stocks, set n_component temporarily to {len(stocks)}')
            ind_sample_demean = sample_demean.loc[:, stocks].dropna(axis=1)
            ind_sample_cov = ind_sample_demean.T.dot(ind_sample_demean) / (ind_sample_demean.shape[0] - 1)
            eigvalue, _ = np.linalg.eigh(ind_sample_cov)
            if n_component < 1:
                comp_contrib.loc[date, ind] = 1 - eigvalue[-1:-int(math.ceil(n_component * len(stocks)))-1:-1].sum() / eigvalue.sum()
            else:
                comp_contrib.loc[date, ind] = 1 - eigvalue[-1:-min(n_component, len(stocks))-1:-1].sum() / eigvalue.sum()
    return comp_contrib


if __name__ == "__main__":
    close_price = pd.read_csv('assets/data/stock.nosync/daily/adj_close.csv', index_col=0, parse_dates=True)
    com_con = calc_component_contribution(close_price, n_component=1, window=60)
    com_con.to_csv('absorb_ratio/result/compcontrib.csv')