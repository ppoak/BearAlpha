import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from ..tools import *


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
    
    def __init__(self, factor: PanelFrame, 
        forward: PanelFrame = None, 
        group: PanelFrame = None, 
        price: PanelFrame = None, infer_forward: 'str | list' = None):
        '''Analyzer is a general analyst dedicated to analyze the AnalyzeData
        ---------------------------------------------------------------------

        factor: panelframe, the factor data
        forward: panelframe, the forward data
        group: panelframe, the group data
        price: panelframe, the price data
        '''
        self.factor = factor
        self.forward = forward
        self.group = group
        self.price = price
        
        if forward is not None and infer_forward is not None and price is not None:
            print('[!] You set infer forward but at the same time forward, so the forward'
            'parameter will no longer be used.')
        if infer_forward is not None and price is not None:
            price_types = self.price.indicators
            is_single = len(price_types) == 1
            has_open = 'open' in price_types
            has_close = 'close' in price_types
            if is_single:
                self.forward = PanelFrame(indicators=price2fwd(
                    self.price.cut(indicator=price_types[0]), self.price.cut(indicator=price_types[0]),
                    infer_forward))
            elif has_open and has_close:
                self.forward = PanelFrame(indicators=price2fwd(
                    self.price.cut(indicator='open'), 
                    self.price.cut(indicator='close'),
                    infer_forward))
            elif not has_close and has_open:
                self.forward = PanelFrame(indicators=price2fwd(
                    self.price.cut(indicator='open'), 
                    self.price.cut(indicator='open'), 
                    infer_forward))
            elif not has_open and has_close:
                self.forward = PanelFrame(indicators=price2fwd(
                    self.price.cut(indicator='close'), 
                    self.price.cut(indicator='close'), 
                    infer_forward))
            else:
                raise ValueError('[!] You set infer forward but no we cannot calculate.')
        
    def regression(self, factor: 'str | list' = None, period: 
            'str | list' = None, group: 'str | list' = None):
        '''Barra Regression Model
        -----------------------

        factor: str, the factor name
        period: str, the period of regression
        '''
        def _reg(d):
            dg = category2dummy(d.loc[:, 'group'], drop_first=True)
            x = pd.concat([d.loc[:, 'factor'], dg], axis=1)
            x = sm.add_constant(x)
            y = d.loc[:, 'forward']
            model = sm.OLS(y, x).fit()
            t = model.tvalues['factor']
            coef = model.params['factor']
            return pd.Series({f't': t, f'coef': coef})
            
        if isinstance(factor, str):
            factor = [factor]
        if isinstance(period, str):
            period = [period]
        if isinstance(group, str):
            group = [group]

        if factor is None:
            factor = self.factor.indicators
        if period is None:
            period = self.forward.indicators
        if group is None:
            group = self.group.indicators
        
        results = {}
        for f in factor:
            for p in period:
                for g in group:
                    reg_data = pd.concat([
                        self.factor.cut(indicator=f).stack(),
                        self.forward.cut(indicator=p).stack(),
                        self.group.cut(indicator=g).stack()],
                        axis=1).dropna()
                    reg_data.columns = ['factor', 'forward', 'group']
                    results[f"{f}_{p}_{g}"] = reg_data.groupby(level=0).apply(_reg)
        
        results = PanelFrame(indicators=results)
        self.reg_result = results
        return results

    def ic(self, factor: 'str | list' = None, period: 'str | list' = None):
        '''IC value
        ----------

        factor: str, the factor name
        period: str, the period of regression
        '''
        def _ic(d):
            cor = d.corr(method='spearman')
            return pd.Series({f'ic': cor.loc['factor', 'forward']})
            
        if isinstance(factor, str):
            factor = [factor]
        if isinstance(period, str):
            period = [period]

        if factor is None:
            factor = self.factor.indicators
        if period is None:
            period = self.forward.indicators
        
        results = {}
        for f in factor:
            for p in period:
                ic_data = pd.concat([
                    self.factor.cut(indicator=f).stack(), self.forward.cut(indicator=p).stack()
                ], axis=1).dropna()
                ic_data.columns = ["factor", "forward"]
                results[f"{f}_{p}"] = ic_data.groupby(level=0).apply(_ic)
        
        results = PanelFrame(indicators=results)
        self.ic_result = results
        return results

    def layering(self, factor: 'str | list' = None, period: 'str | list' = None, quantiles: int = 5):
        '''Layering value
        ----------------

        factor: str, the factor name
        period: str, the period of regression
        '''
        def _layering(d):
            profit = d.groupby(level=0).apply(lambda x:x.loc[:, 'forward'].mean())
            profit = profit.shift(1).fillna(0)
            cumprofit = (profit + 1).cumprod()
            return pd.DataFrame({"profit": profit, "cumprofit": cumprofit})

        self.quantiles = quantiles
            
        if isinstance(factor, str):
            factor = [factor]
        if isinstance(period, str):
            period = [period]

        if factor is None:
            factor = self.factor.indicators
        if period is None:
            period = self.forward.indicators
        
        results = {}
        for f in factor:
            for p in period:
                layering_data = pd.concat([
                    self.factor.cut(indicator=f).stack(), 
                    self.forward.cut(indicator=p).stack()], axis=1).dropna()
                layering_data.columns = ["factor", "forward"]
                layering_data.loc[:, 'quantiles'] = layering_data.loc[:, 'factor'].groupby(level=0).apply(
                    pd.qcut, q=quantiles, duplicates='drop', labels=False) + 1
                results[f'{f}_{p}'] = PanelFrame(dataframe=layering_data.groupby('quantiles').apply(_layering).swaplevel().sort_index())
        
        self.layering_result = results
        return results

    def regression_plot(self, path: str = None, show: bool = True):
        '''Plot the Regression Result
        -------------------------------

        path: str, path to save the image, default not to save
        show: bool, wheather to show the image
        '''
        _, ax = plt.subplots(figsize=(12, 8))
        self.reg_result.draw(kind='line', asset='t', ax=ax)
        self.reg_result.draw(kind='bar', asset='coef', ax=ax.twinx())
        
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
        self.ic_result.draw(kind='bar', asset='ic', ax=ax)
        
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
        n_factor = len(self.layering_result)
        cols = int(np.sqrt(n_factor)) + 1
        rows = cols - (cols ** 2 - n_factor) // cols

        _, axes = plt.subplots(nrows=rows, ncols=cols, figsize=(12 * cols, 8 * rows))
        axes = np.array(axes).reshape((rows, cols))
        
        for i, (indicator, result) in enumerate(self.layering_result.items()):
            ax = axes[i // cols, i % cols]
            result.draw(kind='bar', indicator='profit', ax=ax, title=f'layering {indicator}')
            result.draw(kind='line', indicator='cumprofit', ax=ax.twinx(), title=f'layering {indicator}')
        
        if path:
            plt.savefig(path)
        if show:
            plt.show()
    
    
if __name__ == "__main__":
    import numpy as np
    indicators = dict(zip(
        [f'indicator{i + 1}' for i in range(5)],
        [pd.DataFrame(np.random.rand(100, 5), index=pd.date_range('2020-01-01', periods=100), columns=list('abcde')) for _ in range(5)]
        ))
    prices = dict(zip(
        list('abcde'),
        [pd.DataFrame(np.random.rand(100, 2), index=pd.date_range('2020-01-01', periods=100), columns=['open', 'close']) for _ in range(5)]
    ))
    groups = dict(zip(
        pd.date_range('2020-01-01', periods=100),
        [pd.DataFrame(['A', 'B', 'C', 'A', 'D'], index=list('abcde'), columns=['groups']) for _ in range(100)]
    ))
    
    factors = PanelFrame(indicators=indicators)
    prices = PanelFrame(assets=prices)
    groups = PanelFrame(datetimes=groups)

    analyzer = Analyzer(factor=factors, price=prices, group=groups, infer_forward='5d')
    analyzer.regression()
    analyzer.regression_plot(show=True)
