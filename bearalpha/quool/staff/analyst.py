import numpy as np
import pandas as pd
from ..tools import *


class AnalystError(FrameWorkError):
    pass


@pd.api.extensions.register_dataframe_accessor("regressor")
@pd.api.extensions.register_series_accessor("regressor")
class Regressor(Worker):
    '''Regressor is a staff worker in quool, used for a dataframe
    to perform regression analysis in multiple ways, like ols, logic,
    and so on.
    '''
    
    def ols(self, 
        y: pd.Series, 
        intercept: bool = True,
        backend: str = 'statsmodels',
        **kwargs
        ):
        '''OLS Regression Function
        ---------------------------

        y: Series, assigned y value in a series form
        intercept: bool, whether to add a intercept value
        kwargs: some other kwargs passed to backend
        '''
        def _statsmodels_reg(x, y):
            if intercept:
                x = add_constant(x)
            model = OLS(y, x).fit(**kwargs)
            return model
        
        def _sklearn_reg(x, y):
            model = LinearRegression(fit_intercept=intercept, **kwargs)
            model.fit(x, y)
            return model

        if backend == 'statsmodels':
            from statsmodels.api import OLS, add_constant
            if self.type_ == Worker.PN:
                return self.data.copy().groupby(level=0).apply(
                    lambda x: _statsmodels_reg(x, y.loc[x.index]))
            else:
                return _statsmodels_reg(self.data, y)
        elif backend == 'sklearn':
            from sklearn.linear_model import LinearRegression
            if self.type_ == Worker.PN:
                return self.data.copy().groupby(level=0).apply(
                    lambda x: _sklearn_reg(x, y.loc[x.index]))
            else:
                return _sklearn_reg(self.data, y)
        
    def logistic(self, 
        y: pd.Series = None, 
        intercept: bool = True,
        backend: str = 'statsmodels',
        **kwargs
        ):
        '''Logistics Regression Function
        ---------------------------

        y: Series, assigned y value in a series form
        intercept: bool, whether to add a intercept value
        backend: str, choose between statsmodels and sklearn
        kwargs: some other kwargs passed to backend
        '''
        def _statsmodels_reg(x, y):
            if intercept:
                x = add_constant(x)
            model = Logit(y, x).fit(**kwargs)
            return model
        
        def _sklearn_reg(x, y):
            model = LogisticRegression(fit_intercept=intercept, **kwargs)
            model.fit(x, y)
            return model

        if backend == 'statsmodels':
            from statsmodels.api import Logit, add_constant
            if self.type_ == Worker.PN:
                return self.data.copy().groupby(level=0).apply(
                    lambda x: _statsmodels_reg(x, y.loc[x.index]))
            else:
                return _statsmodels_reg(self.data.copy(), y)
        
        elif backend == 'sklearn':
            from sklearn.linear_model import LogisticRegression
            if self.type_ == Worker.PN:
                return self.data.copy().groupby(level=0).apply(
                    lambda x: _sklearn_reg(x, y.loc[x.index]))
            else:
                return _sklearn_reg(self.data.copy(), y)


@pd.api.extensions.register_dataframe_accessor("decompositer")
@pd.api.extensions.register_series_accessor("decompositer")
class Decompositer(Worker):

    def pca(self, ncomp: int, backend: str = 'statsmodels', **kwargs):
        if backend == 'statsmodels':
            from statsmodels.multivariate.pca import PCA
            if self.type_ == Worker.PN:
                return self.data.copy().groupby(level=0).apply(
                    lambda x: PCA(x, ncomp=ncomp, **kwargs))
            else:
                return PCA(self.data.copy(), ncomp=ncomp, **kwargs)
        
        elif backend == 'sklearn':
            from sklearn.decomposition import PCA
            if self.type_ == Worker.PN:
                return self.data.copy().groupby(level=0).apply(
                    lambda x: PCA(n_components=ncomp, **kwargs).fit(x))
            else:
                return PCA(n_components=ncomp, **kwargs).fit(self.data.copy())


@pd.api.extensions.register_dataframe_accessor("describer")
@pd.api.extensions.register_series_accessor("describer")
class Describer(Worker):
    '''Describer is a staff worker in quool, used for a dataframe
    or a series to perform a series of descriptive analysis, like
    correlation analysis, and so on.
    '''

    def corr(self, other: pd.Series = None, method: str = 'spearman', tvalue = False):
        '''Calculation for correlation matrix
        -------------------------------------

        method: str, the method for calculating correlation function
        tvalue: bool, whether to return t-value of a time-seriesed correlation coefficient
        '''
        if other is not None:
            other = other.copy()
            if self.type_ == Worker.PN:
                other.index.names = self.data.index.names
            else:
                other.index.name = self.data.name
            
            if self.data.name is None:
                self.data.name = 'corr_x'
            if other.name is None:
                other.name = 'corr_y'
            
            data = pd.merge(self.data, other, left_index=True, right_index=True)
        else:
            data = self.data

        if self.type_ == Worker.PN:
            corr = data.groupby(level=0).corr(method=method)
            if tvalue:
                n = corr.index.levels[0].size
                mean = corr.groupby(level=1).mean()
                std = corr.groupby(level=1).std()
                t = mean / std * np.sqrt(n)
                t = t.loc[t.columns, t.columns].replace(np.inf, np.nan).replace(-np.inf, np.nan)
                return t
            return corr
        else:
            return data.corr(method=method)

    def ic(self, forward: pd.Series = None, grouper = None, method: str = 'spearman'):
        '''To calculate ic value
        ------------------------

        other: series, the forward column
        method: str, 'spearman' means rank ic
        '''
        if forward is not None:
            forward = forward.copy()
            if self.type_ == Worker.PN:
                forward.index.names = self.data.index.names
            else:
                forward.index.name = self.data.index.name
            
            if not self.is_frame and self.data.name is None:
                self.data.name = 'factor'
            if isinstance(forward, pd.Series) and forward.name is None:
                forward.name = 'forward'
            data = pd.merge(self.data, forward, left_index=True, right_index=True)
        else:
            data = self.data.copy()
        
        groupers = [pd.Grouper(level=0)]
        if grouper is not None:
            groupers += item2list(grouper)
        groupers_num = len(groupers)
            
        if self.type_ == Worker.PN:
            ic = data.groupby(groupers).corr(method=method)
            idx = (slice(None),) * groupers_num + (ic.columns[-1],)
            ic = ic.loc[idx, ic.columns[:-1]].droplevel(groupers_num)
            return ic

        elif self.type_ == Worker.CS:
            if groupers_num < 2:
                ic = data.corr(method=method)
            else:
                ic = data.groupby(groupers[1:]).corr(method=method)
            idx = (slice(None),) * (groupers_num - 1) + (ic.columns[-1],)
            if groupers_num < 2:
                ic = ic.loc[idx, ic.columns[:-1]]
            else:
                ic = ic.loc[idx, ic.columns[:-1]].droplevel(groupers_num - 1)
            return ic
        
        else:
            raise AnalystError('ic', 'Timeseries data cannot be used to calculate ic value!')


@pd.api.extensions.register_dataframe_accessor("tester")
@pd.api.extensions.register_series_accessor("tester")
class Tester(Worker):

    def sigtest(self, h0: 'float | pd.Series' = 0):
        '''To apply significant test (t-test, p-value) to see if the data is significant
        -------------------------------------------------------------------------

        h0: float or Series, the hypothesized value
        '''
        from scipy.stats import ttest_1samp, ttest_ind
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
            
        if self.type_ == Worker.PN:
            return self.data.groupby(level=1).apply(_t)
        
        else:
            return _t(self.data)


if __name__ == "__main__":
    panelframe = pd.DataFrame(np.random.rand(500, 5), index=pd.MultiIndex.from_product(
        [pd.date_range('20100101', periods=100), list('abcde')]
    ), columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    panelseries = pd.Series(np.random.rand(500), index=pd.MultiIndex.from_product(
        [pd.date_range('20100101', periods=100), list('abcde')]
    ), name='id1')
    tsframe = pd.DataFrame(np.random.rand(500, 5), index=pd.date_range('20100101', periods=500),
        columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    tsseries = pd.Series(np.random.rand(500), index=pd.date_range('20100101', periods=500), name='id1')
    csframe = pd.DataFrame(np.random.rand(5, 5), index=list('abcde'), 
        columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    csseries = pd.Series(np.random.rand(5), index=list('abcde'), name='id1')

    # print(panelframe.regressor.logistic(panelseries, backend='statsmodels').apply(lambda x: x.params))
    print(panelframe.tester.sigtest(panelseries))
