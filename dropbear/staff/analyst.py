import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from .cleaner import Data, DataCollection, AnalyzeData
from .artist import Drawer


class Analyzer(object):
    '''Analyzer is a general analyst dedicated to analyze the AnalyzeData
    =====================================================================

    Analyzer takes one collection of data in AnalyzeData class, heriting from
    DataCollection. There mainly three method of analyzing factor data:

    1. Barra Regression Model
    2. IC value
    3. Layering value

    They can be easily used by calling the method of the class.    
    '''
    
    def __init__(self, data: 'AnalyzeData'):
        '''Analyzer is a general analyst dedicated to analyze the AnalyzeData
        ---------------------------------------------------------------------

        collection: AnalyzeData, the collection of data
        '''
        self.data = data
        self.df = data.df
        self.factor = data.factor.df
        self.forward = data.forward.df
        self.price = data.price.df
        self.group = data.group.df
    
    def regression(self, factor: 'str | list' = None, period: 
            'str | list' = None, group: 'str | list' = None):
        '''Barra Regression Model
        -----------------------

        factor: str, the factor name
        period: str, the period of regression
        '''
        def _reg(d):
            d = d.copy()
            dg = pd.get_dummies(d.loc[:, ('group', g)]).iloc[:, :-1]
            x = pd.concat([d.loc[:, 'factor'], dg], axis=1)
            x = sm.add_constant(x)
            y = d.loc[:, ('forward', p)]
            model = sm.OLS(y, x).fit()
            t = model.tvalues[f]
            coef = model.params[f]
            return pd.Series({f't': t, f'coef': coef})
            
        if isinstance(factor, str):
            factor = [factor]
        if isinstance(period, str):
            period = [period]
        if isinstance(group, str):
            group = [group]

        if factor is None:
            factor = self.data.factor.indicators
        if period is None:
            period = self.data.forward.indicators
        if group is None:
            group = self.data.group.indicators
        
        results = {}
        for f in factor:
            for p in period:
                for g in group:
                    reg_data = self.df.loc[:, [('factor', f), ('forward', p), ('group', g)]].copy()
                    results[f"{f}_{p}_{g}"] = reg_data.groupby(level='datetime').apply(_reg)
        
        results = Data(**results, name='regression_result')
        self.reg_result = results
        return results

    def ic(self, factor: 'str | list' = None, period: 'str | list' = None):
        '''IC value
        ----------

        factor: str, the factor name
        period: str, the period of regression
        '''
        def _ic(d):
            d = d.copy()
            cor = d.corr(method='spearman')
            return pd.Series({f'ic': cor.loc[('factor', f), ('forward', p)]})
            
        if isinstance(factor, str):
            factor = [factor]
        if isinstance(period, str):
            period = [period]

        if factor is None:
            factor = self.data.factor.indicators
        if period is None:
            period = self.data.forward.indicators
        
        results = {}
        for f in factor:
            for p in period:
                ic_data = self.df.loc[:, [('factor', f), ('forward', p)]].copy()
                results[f"{f}_{p}"] = ic_data.groupby(level='datetime').apply(_ic)
        
        results = Data(**results, name='ic_result')
        self.ic_result = results
        return results

    def layering(self, factor: 'str | list' = None, period: 'str | list' = None, quantiles: int = 5):
        '''Layering value
        ----------------

        factor: str, the factor name
        period: str, the period of regression
        '''
        def _layering(d):
            d = d.copy()
            profit = d.groupby(level='datetime').apply(lambda x:x.loc[:, ('forward', p)].mean())
            profit = profit.shift(1).fillna(0)
            cumprofit = (profit + 1).cumprod()
            return pd.DataFrame({"profit": profit, "cumprofit": cumprofit})

        self.quantiles = quantiles
            
        if isinstance(factor, str):
            factor = [factor]
        if isinstance(period, str):
            period = [period]

        if factor is None:
            factor = self.data.factor.indicators
        if period is None:
            period = self.data.forward.indicators
        
        results = []
        for f in factor:
            for p in period:
                layering_data = self.df.loc[:, [('factor', f), ('forward', p)]].copy()
                layering_data.loc[:, 'quantiles'] = layering_data.loc[:, ('factor', f)].groupby(level='datetime').apply(
                    pd.qcut, q=quantiles, duplicates='drop', labels=False) + 1
                results.append(Data(layering_data.groupby('quantiles').apply(_layering).swaplevel().sort_index(), name=f"{f}_{p}"))
        
        results = DataCollection(*results)

        self.layering_result = results
        return results

    def regression_plot(self, path: str = None, show: bool = True):
        '''Plot the Regression Result
        -------------------------------

        path: str, path to save the image, default not to save
        show: bool, wheather to show the image
        '''
        _, ax = plt.subplots(figsize=(12, 8))
        self.reg_result.draw(
            Drawer(method='line', asset='t', ax=ax),
            Drawer(method='bar', asset='coef', ax=ax.twinx()),
            ax=True, path=None, show=False
            )
        
        if path:
            plt.savefig(path)
        if show:
            plt.show()
            
    def ic_plot(self, path: str = None, show: bool = True):
        '''Plot the IC value Result
        -------------------------------

        path: str, path to save the image, default not to save
        show: bool, wheather to show the image
        '''
        _, ax = plt.subplots(figsize=(12, 8))
        self.ic_result.draw(
            Drawer(method='bar', asset='ic', ax=ax),
            ax=True, path=None, show=False
            )
        
        if path:
            plt.savefig(path)
        if show:
            plt.show()
    
    def layering_plot(self, path: str = None, show: bool = True):
        '''Plot the Layering Result
        -----------------------------

        path: str, path to save the image, default not to save
        show: bool, wheather to show the image
        '''
        n_factor = len(self.layering_result.names)
        cols = int(np.sqrt(n_factor)) + 1
        rows = cols - (cols ** 2 - n_factor) // cols
        _, axes = plt.subplots(nrows=rows, ncols=cols, figsize=(12 * cols, 8 * rows))
        params = [[
                Drawer(method='bar', name=name, indicator='profit', ax=axes[i // cols, i % cols], title=f'layering {name}'),
                Drawer(method='line', name=name, indicator='cumprofit', ax=axes[i // cols, i % cols].twinx(), title=f'layering {name}')
            ] for i, name in enumerate(self.layering_result.names)]
        self.layering_result.gallery(*params, path=path, show=show, axes=axes, shape=(rows, cols))
        
    
if __name__ == "__main__":
    import numpy as np
    close_price = pd.DataFrame(np.random.rand(100, 5), index=pd.date_range('20210101', periods=100),
        columns=['a', 'b', 'c', 'd', 'e'])
    factors0 = pd.DataFrame(np.random.rand(500, 2), index=pd.MultiIndex.from_product(
        [pd.date_range('20210101', periods=100), list('abcde')]),
        columns=['factor1', 'factor2'])
    factors1 = pd.DataFrame(np.random.rand(100, 5), index=pd.date_range('20210101', periods=100),
        columns=['a', 'b', 'c', 'd', 'e'])
    factors2 = pd.DataFrame(np.random.rand(500, 2), index=pd.MultiIndex.from_product(
        [pd.date_range('20210101', periods=100), list('abcde')]),
        columns=['factor4', 'factor5']
    )
    group = pd.Series(['A', 'A', 'B', 'B', 'C'] * 100, index=pd.MultiIndex.from_product(
        [pd.date_range('20210101', periods=100), list('abcde')]), name='group')
    
    factors = Data(factors0, factors2, name='factor', factor3=factors1)
    close = Data(name='close', close=close_price)
    group = Data(group, name='group')
    data = AnalyzeData(factor=factors, price=close, group=group, infer_forward=['1d', '10d'])
    
    analyzer = Analyzer(data)
    analyzer.regression()
    analyzer.regression_plot(path='test.png')
