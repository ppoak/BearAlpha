import numpy as np
import pandas as pd
from .base import *
from ..tools import *


class AnalystError(FrameWorkError):
    pass


@pd.api.extensions.register_dataframe_accessor("regressor")
@pd.api.extensions.register_series_accessor("regressor")
class Regressor(Worker):
    """Regressor
    ============

    Regressor is a staff worker in quool, used for a dataframe
    to perform regression analysis in multiple ways
    
    now it supports:
    OLS, linear logistic, WLS
    """

    def __init__(self, data: 'pd.DataFrame | pd.Series'):
        super().__init__(data)

    @staticmethod
    def _valid(data: 'pd.DataFrame | pd.Series'):
        """To make data available for regress"""
        return data.dropna()
    
    def ols(
        self, 
        y: pd.Series, 
        intercept: bool = True,
        backend: str = 'statsmodels',
        **kwargs,
    ):
        """OLS Regression Function
        ---------------------------

        y: Series, assigned y value in a series form
        intercept: bool, whether to add a intercept value
        kwargs: some other kwargs passed to backend
        """
        data = self._valid(self.data.copy())
        y = self._valid(y)

        def _statsmodels_reg(x, y):
            common_index = x.index.intersection(y.index)
            x, y = x.loc[common_index], y.loc[common_index]
            if intercept:
                x = add_constant(x)
            model = OLS(y, x).fit(**kwargs)
            return model
        
        def _sklearn_reg(x, y):
            common_index = x.index.intersection(y.index)
            x, y = x.loc[common_index], y.loc[common_index]
            model = LinearRegression(fit_intercept=intercept, **kwargs)
            model.fit(x, y)
            return model

        if backend == 'statsmodels':
            from statsmodels.api import OLS, add_constant
            if self.type_ == Worker.PNFR or self.type_ == Worker.PNSR:
                return data.groupby(level=0).apply(
                    lambda x: _statsmodels_reg(x, y))
            else:
                return _statsmodels_reg(data, y)
        elif backend == 'sklearn':
            from sklearn.linear_model import LinearRegression
            if self.type_ == Worker.PNFR or self.type_ == Worker.PNSR:
                return data.groupby(level=0).apply(
                    lambda x: _sklearn_reg(x, y))
            else:
                return _sklearn_reg(data, y)
        
    def logistic(
        self, 
        y: pd.Series = None, 
        intercept: bool = True,
        backend: str = 'statsmodels',
        **kwargs
    ):
        """Logistics Regression Function
        ---------------------------

        y: Series, assigned y value in a series form
        intercept: bool, whether to add a intercept value
        backend: str, choose between statsmodels and sklearn
        kwargs: some other kwargs passed to backend
        """
        data = self._valid(self.data)
        y = self._valid(y)
        
        def _statsmodels_reg(x, y):
            common_index = x.index.intersection(y.index)
            x, y = x.loc[common_index], y.loc[common_index]
            if intercept:
                x = add_constant(x)
            model = Logit(y, x).fit(**kwargs)
            return model
        
        def _sklearn_reg(x, y):
            common_index = x.index.intersection(y.index)
            x, y = x.loc[common_index], y.loc[common_index]
            model = LogisticRegression(fit_intercept=intercept, **kwargs)
            model.fit(x, y)
            return model

        if backend == 'statsmodels':
            from statsmodels.api import Logit, add_constant
            if self.type_ == Worker.PNSR or self.type_ == Worker.PNFR:
                return data.groupby(level=0).apply(
                    lambda x: _statsmodels_reg(x, y))
            else:
                return _statsmodels_reg(data, y)
        
        elif backend == 'sklearn':
            from sklearn.linear_model import LogisticRegression
            if self.type_ == Worker.PNFR or self.type_ == Worker.PNSR:
                return data.groupby(level=0).apply(
                    lambda x: _sklearn_reg(x, y))
            else:
                return _sklearn_reg(data, y)

    def wls(
        self, 
        y: pd.Series,
        weights: pd.Series,
        intercept: bool = True,
        backend: str = 'statsmodels',
        **kwargs
    ):
        """WLS(weighted least squares) Regression Function
        ---------------------------

        y: Series, assigned y value in a series form
        weights: Series, higher the weight, higher the proportion of
        intercept: bool, whether to add a intercept value
        kwargs: some other kwargs passed to backend
        """
        data = self._valid(self.data)
        y = self._valid(y)
        weights = self._valid(weights)

        def _statsmodels_reg(x, y, weights):
            common_index = x.index.intersection(y.index).intersection(weights.index)
            x, y, weights = x.loc[common_index], y.loc[common_index], weights.loc[common_index]
            if intercept:
                x = add_constant(x)
            model = WLS(y, x, weights=weights).fit(**kwargs)
            return model
        
        def _sklearn_reg(x, y, weights):
            common_index = x.index.intersection(y.index).intersection(weights.index)
            x, y, weights = x.loc[common_index], y.loc[common_index], weights.loc[common_index]
            model = LinearRegression(fit_intercept=intercept, **kwargs)
            model.fit(x, y, sample_weight=weights)
            return model

        if backend == 'statsmodels':
            from statsmodels.api import WLS, add_constant
            if self.type_ == Worker.PNSR or self.type_ == Worker.PNFR:
                return data.groupby(level=0).apply(
                    lambda x: _statsmodels_reg(x, y, weights))
            else:
                return _statsmodels_reg(data, y, weights)
        elif backend == 'sklearn':
            from sklearn.linear_model import LinearRegression
            if self.type_ == Worker.PNSR or self.type_ == Worker.PNFR:
                return data.groupby(level=0).apply(
                    lambda x: _sklearn_reg(x, y, weights))
            else:
                return _sklearn_reg(data, y, weights)

@pd.api.extensions.register_dataframe_accessor("decompositer")
@pd.api.extensions.register_series_accessor("decompositer")
class Decompositer(Worker):

    def pca(
        self, 
        ncomp: int, 
        backend: str = 'statsmodels', **kwargs
    ):
        """PCA decomposite
        --------------------
        
        ncomp: int, number of components after decomposition
        backend: str, choice between 'sklearn' and 'statsmodels'
        """
        if backend == 'statsmodels':
            from statsmodels.multivariate.pca import PCA
            if self.type_ == Worker.PNSR or self.type_ == Worker.PNFR:
                return self.data.copy().groupby(level=0).apply(
                    lambda x: PCA(x, ncomp=ncomp, **kwargs))
            else:
                return PCA(self.data.copy(), ncomp=ncomp, **kwargs)
        
        elif backend == 'sklearn':
            from sklearn.decomposition import PCA
            if self.type_ == Worker.PNSR or self.type_ == Worker.PNFR:
                return self.data.copy().groupby(level=0).apply(
                    lambda x: PCA(n_components=ncomp, **kwargs).fit(x))
            else:
                return PCA(n_components=ncomp, **kwargs).fit(self.data.copy())


@pd.api.extensions.register_dataframe_accessor("describer")
@pd.api.extensions.register_series_accessor("describer")
class Describer(Worker):
    """Describer is a staff worker in quool, used for a dataframe
    or a series to perform a series of descriptive analysis, like
    correlation analysis, and so on.
    """

    def corr(
        self, 
        other: pd.Series = None, 
        method: str = 'spearman', 
        axis: int = 0,
        tvalue = False,
    ):
        """Calculation for correlation matrix
        -------------------------------------

        method: str, the method for calculating correlation function
        tvalue: bool, whether to return t-value of a time-seriesed correlation coefficient
        """
        if other is not None:
            if self.type_ == Worker.PNSR or self.type_ == Worker.PNFR:
                corr = self.data.groupby(level=0).corrwith(other, method=method)
                if tvalue:
                    t = corr.groupby(level=1).mean() / corr.groupby(level=1).st() \
                        * np.sqrt(corr.index.levels[0].size)
                    return t
                return corr
            else:
                return self.data.corrwith(other, method=method, axis=axis)
        
        else:
            if self.type_ == Worker.PNSR or self.type_ == Worker.PNFR:
                corr = self.data.groupby(level=0).corr(method=method)
                if tvalue:
                    t = corr.groupby(level=1).mean() / corr.groupby(level=1).st() \
                        * np.sqrt(corr.index.levels[0].size)
                    return t
                return corr
            else:
                return self.data.corr(method=method)

    def ic(
        self, 
        ret: pd.Series, 
        grouper = None, 
        method: str = 'spearman'
    ):
        """To calculate ic value
        ------------------------

        other: series, the forward column
        method: str, 'spearman' means rank ic
        """
       
        groupers = [pd.Grouper(level=0)]
        if grouper is not None:
            groupers += item2list(grouper)
        groupers_num = len(groupers)
            
        if self.type_ == Worker.PNSR or self.type_ == Worker.PNFR:
            ic = self.data.groupby(groupers).corrwith(ret, method=method, axis=0)
            idx = (slice(None),) * groupers_num + (ic.columns[-1],)
            ic = ic.loc[idx, ic.columns[:-1]].droplevel(groupers_num)
            return ic

        elif self.type_ == Worker.CSFR or self.type_ == Worker.CSSR:
            if groupers_num < 2:
                ic = self.data.corrwith(ret, method=method)
            else:
                ic = self.data.groupby(groupers[1:]).corrwith(ret, method=method)
            idx = (slice(None),) * (groupers_num - 1) + (ic.columns[-1],)
            if groupers_num < 2:
                ic = ic.loc[idx, ic.columns[:-1]]
            else:
                ic = ic.loc[idx, ic.columns[:-1]].droplevel(groupers_num - 1)
            return ic
        
        else:
            raise AnalystError('ic', 'Timeseries data cannot be used to calculate ic value!')


@pd.api.extensions.register_dataframe_accessor("sigtester")
@pd.api.extensions.register_series_accessor("sigtester")
class SigTester(Worker):

    def ttest(
        self, 
        h0: 'float | pd.Series' = 0
    ):
        """To apply significant test (t-test, p-value) to see if the data is significant
        -------------------------------------------------------------------------

        h0: float or Series, the hypothesized value
        """
        from scipy.stats import ttest_1samp, ttest_ind
        data = self.data.copy()
        def _t(data):            
            if isinstance(h0, (int, float)):
                if isinstance(data, pd.DataFrame):
                    result = data.apply(lambda x: ttest_1samp(x, h0)).T
                    result.columns = ['t', 'p']
                elif isinstance(data, pd.Series):
                    result = ttest_1samp(data, h0)[:]
                    result = pd.Series(result, index=['t', 'p'], name=f'{data.name}_test')

            elif isinstance(h0, pd.Series):
                if isinstance(data, pd.DataFrame):
                    # F-test undone
                    result = data.apply(lambda x: ttest_ind(x, h0)).T
                    result.columns = ['t', 'p']
                elif isinstance(data, pd.Series):
                    # F-test undone
                    result = ttest_ind(data, h0)[:]
                    result = pd.Series(result, index=['t', 'p'], name=f'{data.name}_test')
                    
            else:
                raise AnalystError('sigtest', 'only int/float/pd.Series avaiable for h0')
            return result
            
        if self.type_ == Worker.PNSR or self.type_ == Worker.PNFR:
            return data.groupby(level=1).apply(_t)
        
        else:
            return _t(data)
