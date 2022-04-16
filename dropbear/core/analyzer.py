import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from .base import DataCollection, Data, Drawer


class AnalyzeData(DataCollection):
    '''A Collection of the Standarized Data Used for Analysis
    ========================================================
    
    AnalyzeData can only contain Data class data,
    and specifically, DataCollection is used for getting ready
    for subsequent analysis.
    '''
    
    def __init__(self, factor: 'Data', 
        forward: 'Data' = None, 
        price: 'Data' = None, 
        group: 'Data' = None,
        infer_forward: 'str | list' = None, *args) -> 'None':
        '''DataCollection is used for getting ready for subsequent analysis
        --------------------------------------------------------------------

        factor: Data, the factor data
        forward: Data or None, the forward data
        price: Data or None, the price data
        group: Data or None, the group data
        infer_forward: str or list, the forward data will be inferred from
        '''
        super().__init__(*args)
        if isinstance(infer_forward, str):
            infer_forward = [infer_forward]

        if infer_forward is not None and price is not None:
            if forward is not None:
                print(f'[!] forward is not None, infer_forward {infer_forward} cover forward')
            forward = self.__infer_forward_from_price(price, infer_forward)
        
        self.factor = factor
        self.forward = forward if forward is not None else Data(name='forward')
        self.price = price if price is not None else Data(name='price')
        self.group = group if group is not None else Data(name='group')
        self.data_dict.update({
            "factor": self.factor,
            "forward": self.forward,
            "price": self.price,
            "group": self.group
        })

    def __infer_forward_from_price(self, price: 'Data', infer_forward: 'list') -> 'pd.Series':
        '''infer forward return from ohlc price data'''
        def _infer(start_name: 'str', end_name: 'str') -> 'dict':
            forward = {}
            for iff in infer_forward:
                price_start = price[start_name].resample(iff).first()
                price_end = price[end_name].resample(iff).last()
                forward[iff] = (price_end - price_start) / price_start
            return forward

        price = price.copy()
        has_close = price.get('close', False)
        has_open = price.get('open', False)
        if len(price) == 1:
            forward = _infer(price.indicators[0], price.indicators[0])
        elif not has_close and not has_open:
            raise ValueError('Ambiguous infer forward, Please calculate manually')
        elif not has_close and has_open:
            print('[!] Not find close price, infer forward from open price')
            forward = _infer('open', 'open')
        elif has_close and not has_open:
            print('[!] Not find open price, use close price to infer forward')
            forward = _infer('close', 'close')
        else:
            forward = _infer('open', 'close')

        return Data(name="forward", **forward)

    def __bool__(self) -> bool:
        return self.price.__bool__() or self.forward.__bool__()

class Aanalyzer(object):
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
        
        if show:
            plt.show()
        if path:
            plt.savefig(path)
            
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
        
        if show:
            plt.show()
        if path:
            plt.savefig(path)
    
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
    a = pd.DataFrame(np.random.rand(100, 5), index=pd.date_range('20210101', periods=100),
        columns=['a', 'b', 'c', 'd', 'e'])
    b = pd.DataFrame(np.random.rand(500, 5), index=pd.MultiIndex.from_product(
        [pd.date_range('20210101', periods=100), list('abcde')]),
        columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    c = pd.Series(np.random.rand(500), index=pd.MultiIndex.from_product(
        [pd.date_range('20210101', periods=100), list('abcde')]), name='id6')
    data = AnalyzeData(factor=Data(b), forward=Data(m1=a), group=Data(c))
    analyzer = Aanalyzer(data)
    analyzer.layering()
    analyzer.layering_plot(path='test.png')
