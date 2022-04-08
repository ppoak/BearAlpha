import math
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from utils.io import track


class Centralization():

    def __init__(self, data_path: str, n_component: int, window: int = None, method: str = 'raw') -> None:
        self.n_component = n_component
        self.window = window
        self.ret = self.__get_ret(data_path, window, method)
        
    def __get_ret(self, filepath: str, window: int, method: str) -> pd.DataFrame:
        '''Get citics index return dataframe.

        filepath: str, filepath of citics index data
        window: int, if using "raw" method, window is not necceary
            if using "ewm" method, window is the half-life of the exponential weighting
            if using "avg" method, window is the window size for calculating mean value
        method: str, method of calculating return, "raw" or "ewm" or "avg"
        '''
        close_price = pd.read_csv(filepath, index_col=0, parse_dates=True).iloc[:, :-1]
        raw_ret = (close_price - close_price.shift(1)) / close_price.shift(1)
        if method == 'raw':
            return raw_ret.dropna(how='all')
        elif method == 'ewm':
            return raw_ret.ewm(halflife=window).mean().dropna(how='all')
        elif method == 'avg':
            return raw_ret.rolling(window).mean().dropna(how='all')
        else:
            raise ValueError('Method should be "raw", "ewm" or "avg"')

    def __preprocess_ret(self, ret_win: pd.DataFrame) -> pd.DataFrame:
        '''Preprocess ret, return the covariance matrix in the given time window

        ret_win: pd.DataFrame, return matrix in a given time window
        return: pd.DataFrame, covariance matrix
        '''
        centralize = ret_win - ret_win.mean(axis=0)
        ret_cov = centralize.T.dot(centralize) / (centralize.shape[0] - 1)
        return ret_cov

    def __absorption_ratio(self, ret_win: pd.DataFrame):
        '''Absorption ratio calculating.

        ret_win: pd.DataFrame, asset return in a given time window
        n_component: int, the principal component number when applying pca
        n: int, the nth ar value, if -1 is given, return the total ar value (sum)
        '''
        ret_cov = self.__preprocess_ret(ret_win)
        eigvalue, eigvector = np.linalg.eigh(ret_cov)
        eigvalue = eigvalue[-1:-self.n_component-1:-1]
        eigvector = eigvector[:, -1:-self.n_component-1:-1]
        asset_var = np.diag(ret_cov).sum()
        eigvector_var = eigvector.var(axis=0)
        ar = eigvector_var / asset_var
        self.ar = ar
        # origin vector exposure within eigen vector is sqrt(eigvalue) * eigmatrix
        self.loadmatrix = eigvector * np.sqrt(eigvalue)
            
    def __centralization_ratio(self, ret_win: pd.DataFrame) -> np.ndarray:
        '''Centralization ratio calculating.
        
        ret_win: pd.DataFrame, asset return in a given time window
        return: np.ndarray, centralization ratio
        '''
        self.__absorption_ratio(ret_win)
        ev_weight = np.abs(self.loadmatrix) / np.abs(self.loadmatrix).sum(axis=0)
        ar_weight = self.ar / self.ar.sum()
        centralization_vector = np.dot(ev_weight, ar_weight)
        return centralization_vector
    
    def calc(self) -> pd.DataFrame:
        '''Calculate all period centralization ratio'''
        centralization = pd.DataFrame(columns=self.ret.columns, dtype='float32')
        for idx in track(range(self.window, self.ret.shape[0])):
            sample = self.ret.iloc[idx - self.window:idx]
            centralization.loc[self.ret.index[idx], :] = self.__centralization_ratio(sample)
        return centralization

def calc_centralization(close_price: pd.DataFrame, n_component: int, window: int = None) -> pd.DataFrame:
    '''Calculate centralization ratio for a given time window

    close_price: pd.DataFrame, close price dataframe, **without CI005029.WI**
    n_component: int, the principal component number when applying pca
    window: int, the time window size
    '''
    # preprocess return data into exponetial weighted mean
    ret = (close_price - close_price.shift(1)) / close_price.shift(1)
    ret = ret.ewm(halflife=250).mean()
    
    centralize_data = pd.DataFrame(columns=ret.columns, dtype='float32')
    for idx in track(range(window, ret.shape[0])):
        # take sample by window parameter
        sample = ret.iloc[idx - window:idx]
        sample_demean = sample - sample.mean(axis=0)
        # calculate sample covariance
        sample_cov = sample_demean.T.dot(sample_demean) / (sample_demean.shape[0] - 1)
        # calculate eigenvalue and eigenvector
        eigvalue, eigvector = np.linalg.eigh(sample_cov)
        # take largest n_component eigenvalue and eigenvector
        eigvalue, eigvector = eigvalue[-1:-n_component-1:-1], eigvector[:, -1:-n_component-1:-1]
        # calculate asset variance
        asset_var = np.diag(sample_cov).sum()
        # calculate ar vector
        ar = eigvector.var(axis=0) / asset_var
        # calculate load matrix
        loadmatrix = eigvector * np.sqrt(eigvalue)
        # calculate ev weight and ar weight
        ev_weight = np.abs(loadmatrix) / np.abs(loadmatrix).sum(axis=0)
        ar_weight = ar / ar.sum()
        # calculate centralize ratio
        centralize_vector = np.dot(ev_weight, ar_weight)
        centralize_data.loc[ret.index[idx], :] = centralize_vector
        
    return centralize_data

def calc_industry_ar(close_price: pd.DataFrame, n_component: int, window: int) -> pd.DataFrame:
    ret = (close_price - close_price.shift(1)) / close_price.shift(1)
    ret = ret - ret.mean(axis=0)
    # ret = ret.ewm(halflife=20).mean()
    ind_list_all =  [
        'zx_petro','zx_coal','zx_metals','zx_power','zx_steel','zx_chemicals',
        'zx_construct_eng','zx_construct_mat','zx_light_man','zx_machinery',
        'zx_electr_equip','zx_defense','zx_automobiles','zx_retail','zx_hotels_lei',
        'zx_household_dur','zx_textile','zx_medical','zx_food_bev','zx_agriculture',
        'zx_banks','zx_non_bank_fin','zx_real_estate','zx_transportation','zx_electronic_comp',
        'zx_communication','zx_computers','zx_media'
        ]
    industry_ar = pd.DataFrame(columns=ind_list_all, dtype='float32')
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
            eigvalue, eigvector = np.linalg.eigh(ind_sample_cov)
            if n_component < 1:
                eigvalue, eigvector = eigvalue[-1:-int(math.ceil(n_component * len(stocks)))-1:-1], eigvector[:, -1:-int(math.ceil(n_component * len(stocks)))-1:-1]
            else:
                eigvalue, eigvector = eigvalue[-1:-min(n_component, len(stocks))-1:-1], eigvector[:, -1:-min(n_component, len(stocks))-1:-1]
            asset_var = np.diag(ind_sample_cov).sum()
            eigvector_var = eigvector.var(axis=0)
            ar = eigvector_var / asset_var
            industry_ar.loc[date, ind] = ar.sum()
    return industry_ar


if __name__ == "__main__":
    n_component = 0.1
    window = 60

    close_price = pd.read_csv('assets/data/stock.nosync/daily/adj_close.csv', index_col=0, parse_dates=True)
    ind_ar = calc_industry_ar(close_price, n_component, window)
    ind_ar.to_csv("absorb_ratio/result/industry_10.csv")
    print(ind_ar)

    
    '''
    cent = Centralization('assets/data/industry/citics_close.csv', n_component=n_component, window=window, method=method)
    result = cent.calc()
    result.to_csv(f"absorb_ratio/result/table/ncomponent{n_component}_window{window}_{method}.csv")

    result = result.resample('1m').last().dropna().loc['2020':'2021']
    result.index = result.index.map(lambda x: x.strftime(f'%Y-%m-%d'))
    plt.figure(figsize=(24, 10))
    sns.heatmap(data=result.T, cmap='RdYlGn_r')
    plt.savefig(f'absorb_ratio/result/images/ncomponent{n_component}_window{window}_{method}.png')  
    '''
