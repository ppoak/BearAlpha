import numpy as np
import pandas as pd
from ..core import *
from ..tools import *


class ProcessorError(FrameWorkError):
    pass

@pd.api.extensions.register_dataframe_accessor("converter")
@pd.api.extensions.register_series_accessor("converter")
class Converter(Worker):
    
    def price2ret(
        self, 
        period: 'str | int', 
        open_col: str = 'close', 
        close_col: str = 'close', 
        method: str = 'algret',
        lag: int = 1,
    ):
        """Convert the price information to return information
        ------------------------------------------------------
        
        period: str or int or DateOffset, if in str and DateOffset format,
            return will be in like resample format, otherwise, you can get rolling
            return formatted data
        open_col: str, if you pass a dataframe, you need to assign which column
            represents open price
        colse_col: str, the same as open_col, but to assign close price
        method: str, choose between 'algret' and 'logret'
        lag: int, define how many day as lagged after the day of calculation forward return
        """
        if self.type_ == Worker.PN and self.is_frame:
            # https://pandas.pydata.org/docs/reference/api/pandas.Grouper.html
            # https://stackoverflow.com/questions/15799162/
            if isinstance(period, int):
                if period > 0:
                    close_price = self.data.loc[:, close_col]
                    open_price = self.data.groupby(pd.Grouper(level=1))\
                        .shift(period).loc[:, open_col]
                else:
                    close_price = self.data.groupby(pd.Grouper(level=1))\
                        .shift(period - lag).loc[:, close_col]
                    open_price = self.data.groupby(pd.Grouper(level=1))\
                        .shift(-lag).loc[:, open_col]

            else:
                if '-' in str(period):
                    if isinstance(period, str):
                        period = period.strip('-')
                    else:
                        period = - period
                    # https://pandas.pydata.org/docs/reference/api/pandas.Grouper.html
                    # https://stackoverflow.com/questions/15799162/
                    close_price = self.data.groupby(level=1).shift(-lag).groupby([
                        pd.Grouper(level=0, freq=period, label='left'),
                        pd.Grouper(level=1)
                    ]).last().loc[:, close_col]
                    open_price = self.data.groupby(level=1).shift(-lag).groupby([
                        pd.Grouper(level=0, freq=period, label='left'),
                        pd.Grouper(level=1)
                    ]).first().loc[:, open_col]

                else:
                    close_price = self.data.groupby([
                        pd.Grouper(level=0, freq=period, label='right'),
                        pd.Grouper(level=1)
                    ]).last().loc[:, close_col]
                    open_price = self.data.groupby([
                        pd.Grouper(level=0, freq=period, label='right'),
                        pd.Grouper(level=1)
                    ]).first().loc[:, open_col]

        elif self.type_ == Worker.PN and not self.is_frame:
            # if passing a series in panel form, assuming that
            # it is the only way to figure out a return
            if isinstance(period, int):
                if period > 0:
                    close_price = self.data
                    open_price = self.data.groupby(pd.Grouper(level=1)).shift(period)
                else:
                    close_price = self.data.groupby(pd.Grouper(level=1)).shift(period - lag)
                    open_price = self.data.groupby(pd.Grouper(level=1)).shift(-lag)

            else:
                if '-' in str(period):
                    if isinstance(period, str):
                        period = period.strip('-')
                    else:
                        period = - period
                    # if passing a series in panel form, assuming that
                    # it is the only way to figure out a return
                    close_price = self.data.groupby(level=1).shift(-lag).groupby([
                        pd.Grouper(level=0, freq=period, label='left'),
                        pd.Grouper(level=1)
                    ]).last()
                    open_price = self.data.groupby(level=1).shift(-lag).groupby([
                        pd.Grouper(level=0, freq=period, label='left'),
                        pd.Grouper(level=1)
                    ]).first()
                else:
                    close_price = self.data.groupby([
                        pd.Grouper(level=0, freq=period, label='right'),
                        pd.Grouper(level=1)
                    ]).last()
                    open_price = self.data.groupby([
                        pd.Grouper(level=0, freq=period, label='right'),
                        pd.Grouper(level=1)
                    ]).first()

        # if timeseries data is passed, we assume that the columns are asset names
        elif self.type_ == Worker.TS:
            if isinstance(period, int):
                if period > 0:
                    close_price = self.data
                    open_price = self.data.shift(period)
                else:
                    close_price = self.data.shift(-period - lag)
                    open_price = self.data.shift(-lag)
            
            else:
                if '-' in str(period):
                    if isinstance(period, str):
                        period = period.strip('-')
                    else:
                        period = - period
                    close_price = self.data.shift(-lag).resample(period, label='left').last()
                    open_price = self.data.shift(-lag).resample(period, label='left').first()
                else:
                    close_price = self.data.resample(period, label='right').last()
                    open_price = self.data.resample(period, label='right').first()
            
        else:
            raise ProcessorError('price2ret', 'Can only convert time series data to return')

        if method == 'algret':
            return (close_price - open_price) / open_price
        elif method == 'logret':
            return np.log(close_price / open_price)
        
    def cum2diff(
        self,
        grouper = None, 
        period: int = 1, 
        axis: int = 0, 
        keep: bool = True
    ):
        def _diff(data):
            diff = data.diff(period, axis=axis)
            if keep:
                diff.iloc[:period] = data.iloc[:period]
            return diff
        
        if grouper is None:
            diff = _diff(self.data)
        else:
            diff = self.data.groupby(grouper).apply(lambda x: x.groupby(level=1).apply(_diff))
            
        return diff

    def dummy2category(
        self, 
        dummy_col: list = None, 
        name: str = 'group'
    ):
        if not self.is_frame:
            raise ProcessorError('dummy2category', 'Can only convert dataframe to category')
            
        if dummy_col is None:
            dummy_col = self.data.columns
        
        columns = pd.DataFrame(
            dummy_col.values.reshape((1, -1))\
            .repeat(self.data.shape[0], axis=0),
            index=self.data.index, columns=dummy_col
        )
        # fill nan value with 0, because bool(np.nan) is true
        category = columns[self.data.loc[:, dummy_col].fillna(0).astype('bool')]\
            .replace(np.nan, '').astype('str').sum(axis=1)
        category.name = name
        return category

    def logret2algret(self):
        return np.exp(self.data) - 1
    
    def algret2logret(self):
        return np.log(self.data)

    def resample(self, rule: str, **kwargs):
        if self.type_ == Worker.TS:
            return self.data.resample(rule, **kwargs)
        elif self.type_ == Worker.PN:
            return self.data.groupby([pd.Grouper(level=0, freq=rule, **kwargs), pd.Grouper(level=1)])

@pd.api.extensions.register_dataframe_accessor("preprocessor")
@pd.api.extensions.register_series_accessor("preprocessor")
class PreProcessor(Worker):
    
    def standarize(
        self, 
        method: str = 'zscore', 
        grouper = None
    ):
        def _zscore(data):
            mean = data.mean()
            std = data.std()
            zscore = (data - mean) / std
            return zscore

        def _minmax(data):
            min_ = data.min()
            max_ = data.max()
            minmax = (data - min_) / (max_ - min_)
            return minmax

        if not self.is_frame:
            data = self.data.to_frame().copy()
        else:
            data = self.data.copy()

        if self.type_ == Worker.PN:
            if grouper is not None:
                grouper = [pd.Grouper(level=0)] + item2list(grouper)
            else:
                grouper = pd.Grouper(level=0)

        if 'zscore' in method:
            if self.type_ == Worker.PN:
                return data.groupby(grouper).apply(_zscore)
            elif grouper is not None:
                return data.groupby(grouper).apply(_zscore)
            else:
                return _zscore(data)

        elif 'minmax' in method:
            if self.type_ == Worker.PN:
                return data.groupby(grouper).apply(_minmax)
            elif grouper is not None:
                return data.groupby(grouper).apply(_minmax)
            else:
                return _minmax(data)

    def deextreme(
        self,
        method = 'mad', 
        grouper = None, 
        n = None
    ):

        def _mad(data):
            median = data.median()
            mad = (data - median).abs().median()
            mad = mad.values.reshape((1, -1)).repeat(len(data), axis=0).reshape(data.shape)
            mad = pd.DataFrame(mad, index=data.index, columns=data.columns)
            madup = median + n * mad
            maddown = median - n * mad
            data[data > madup] = madup
            data[data < maddown] = maddown
            return data
            
        def _std(data):
            mean = data.mean()
            mean = mean.values.reshape((1, -1)).repeat(len(data), axis=0).reshape(data.shape)
            mean = pd.DataFrame(mean, index=data.index, columns=data.columns)
            std = data.std()
            up = mean + n * std
            down = mean - n * std
            data[data > up] = up
            data[data < down] = down
            return data
        
        def _drop_odd(data):
            if not isinstance(n, (list, tuple)):
                min_, max_ = n / 2, 1 - n /2
            else:
                min_, max_ = n[0], n[1]
            down = data.quantile(min_)
            up = data.quantile(max_)
            down = down.values.reshape((1, -1)).repeat(len(data), axis=0).reshape(data.shape)
            up = up.values.reshape((1, -1)).repeat(len(data), axis=0).reshape(data.shape)
            data[data > up] = up
            data[data < down] = down
            return data
        
        if not self.is_frame:
            data = self.data.to_frame().copy()
        else:
            data = self.data.copy()
        
        if self.type_ == Worker.PN:
            if grouper is not None:
                grouper = [pd.Grouper(level=0)] + item2list(grouper)
            else:
                grouper = pd.Grouper(level=0)
    
        if 'mad' in method:
            if n is None:
                n = 5
            if self.type_ == Worker.PN:
                return data.groupby(grouper).apply(_mad)
            elif grouper is not None:
                return data.groupby(grouper).apply(_mad)
            else:
                return _mad(data)


        elif 'std' in method:
            if n is None:
                n = 3
            if self.type_ == Worker.PN:
                return data.groupby(grouper).apply(_std)
            elif grouper is not None:
                return data.groupby(grouper).apply(_std)
            else:
                return _std(data)
        
        elif 'drop' in method:
            if n is None:
                n = 0.1
            if self.type_ == Worker.PN:
                return data.groupby(grouper).apply(_drop_odd)
            elif grouper is not None:
                return data.groupby(grouper).apply(_drop_odd)
            else:
                return _drop_odd(data)
    
    def fillna(
        self, 
        method = 'zero', 
        grouper = None
    ):

        def _zero(data):
            data = data.fillna(0)
            return data
            
        def _mean(data):
            mean = data.mean(axis=0)
            mean = mean.values.reshape((1, -1)).repeat(len(data), axis=0).reshape(data.shape)
            mean = pd.DataFrame(mean, columns=data.columns, index=data.index)
            data = data.fillna(mean)
            return data

        def _median(data):
            median = data.median(axis=0)
            median = median.values.reshape((1, -1)).repeat(len(data), axis=0).reshape(data.shape)
            median = pd.DataFrame(median, columns=data.columns, index=data.index)
            data = data.fillna(median)
            return data

        if not self.is_frame:
            data = self.data.to_frame().copy()
        else:
            data = self.data.copy()
        
        if self.type_ == Worker.PN:
            if grouper is not None:
                grouper = [pd.Grouper(level=0)] + item2list(grouper)
            else:
                grouper = pd.Grouper(level=0)

        if 'zero' in method:
            if self.type_ == Worker.PN:
                return data.groupby(grouper).apply(_zero)
            elif grouper is not None:
                return data.groupby(grouper).apply(_zero)
            else:
                return _zero(data)

        elif 'mean' in method:
            if self.type_ == Worker.PN:
                return data.groupby(grouper).apply(_mean)
            elif grouper is not None:
                return data.groupby(grouper).apply(_mean)
            else:
                return _mean(data)
        
        elif 'median' in method:
            if self.type_ == Worker.PN:
                return data.groupby(grouper).apply(_median)
            elif grouper is not None:
                return data.groupby(grouper).apply(_median)
            else:
                return _median(data)

if __name__ == "__main__":
    import numpy as np
    price = pd.DataFrame(np.random.rand(100, 4), columns=['open', 'high', 'low', 'close'],
        index = pd.date_range('20100101', periods=100))
        # index=pd.MultiIndex.from_product([pd.date_range('20100101', periods=20), list('abced')]))
    price.iloc[0, 2] = np.nan
    print(price.preprocessor.standarize('zscore'))
    