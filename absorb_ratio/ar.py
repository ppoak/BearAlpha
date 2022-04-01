import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

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
        for idx in range(self.window, self.ret.shape[0]):
            sample = self.ret.iloc[idx - self.window:idx]
            centralization.loc[self.ret.index[idx], :] = self.__centralization_ratio(sample)
        return centralization
    

if __name__ == "__main__":
    n_component = 30
    window=60
    method='ewm'
    
    cent = Centralization('assets/data/industry/citics_close.csv', n_component=n_component, window=window, method=method)
    result = cent.calc()
    result.to_csv(f"absorb_ratio/result/table/ncomponent{n_component}_window{window}_{method}.csv")

    result = result.resample('1m').last().dropna().loc['2020':'2021']
    result.index = result.index.map(lambda x: x.strftime(f'%Y-%m-%d'))
    plt.figure(figsize=(24, 10))
    sns.heatmap(data=result.T, cmap='RdYlGn_r')
    plt.savefig(f'absorb_ratio/result/images/ncomponent{n_component}_window{window}_{method}.png')