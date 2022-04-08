import numpy as np
from dropbear.define import FactorBase
from utils import *


class Momentum(FactorBase):
    '''Stock total return
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, with_group = True, period = 20):
        super().__init__('momentum', with_group)
        self.name = f'momentum_{period}'
        self.period = period

    def calcuate_factor(self, date):
        # calculate factor
        last_date = last_n_trade_dates(date, self.period)
        market_date = market_daily(self.date, self.date, ['code', 'adjusted_close'])
        market_last = market_daily(last_date, last_date, ['code', 'adjusted_close'])    
        self.factor = (market_date - market_last) / market_last
        self.factor.name = self.name
    
class Turnover(FactorBase):
    '''Stock average turnover MA(VOLUME/CAPITAL, 20)
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, with_group=True, period=20):
        super().__init__('turnover', with_group)
        self.name = f'turnover_{period}'
        self.period = period
    
    def calcuate_factor(self, date):
        # calculate factor
        last_date = last_n_trade_dates(date, self.period)
        turnover = derivative_indicator(last_date, self.date, ['trade_dt', 's_info_windcode', 's_dq_freeturnover'])
        self.factor = turnover.groupby(level=1).mean()
        self.factor.name = self.name

class Volatility(FactorBase):
    '''Stock total return standard deviation over past 20 trading days
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, with_group=True, period=20):
        super().__init__('volatility', with_group)
        self.name = f'volatility{period}'
        self.period = period
    
    def calcuate_factor(self, date):
        # calculate factor
        last_date = last_n_trade_dates(date, self.period)
        market = market_daily(last_date, date, ['trade_date', 'code', 'percent_change'])
        self.factor = market.groupby(level=1).std()
        self.factor = self.name

class Ar(FactorBase):
    '''Sum of (daily high price - daily open price)/(daily open price - daily low price) of previous 20 days
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, with_group=True):
        super().__init__('ar', with_group)
    
    def calcuate_factor(self, date):
        last_date = last_n_trade_dates(date, 20)
        market = market_daily(last_date, date, 
            ['trade_date', 'code', 'adjusted_high', 'adjusted_low', 'adjusted_open'])
        factor = market.groupby(level=1).apply(lambda x:
            ((x.adjusted_high - x.adjusted_open) / (x.adjusted_open - x.adjusted_low)).sum())
        factor = factor.replace(np.inf, np.nan)
        factor.name = 'ar'

class Br(FactorBase):
    '''sum of maximum(0, (high - previous close price)) / sum of maximum(0, (previous close price - low)) of previous 20 days
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, with_group=True):
        super().__init__('br', with_group)
    
    def calcuate_factor(self, date):
        # calculate factor
        last_date = last_n_trade_dates(date, 20)
        market = market_daily(last_date, date, 
            ['trade_date', 'code', 'adjusted_preclose', 'adjusted_high', 'adjusted_low'])
        self.factor = market.groupby(level=1).apply(lambda x:
            (x.loc[:, 'adjusted_high'] - x.loc[:, 'adjusted_preclose']).clip(0).sum() / 
            (x.loc[:, 'adjusted_preclose'] - x.loc[:, 'adjusted_low']).clip(0).sum())
        self.factor.name = 'br'
    
class Bias(FactorBase):
    '''(last close price - 20 days close price moving average) / 20 days close price moving average
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, with_group=True):
        super().__init__('bias', with_group)
    
    def calcuate_factor(self, date):
        # calculate factor
        last_date = last_n_trade_dates(date, 20)
        market = market_daily(last_date, date, 
            ['trade_date', 'code', 'adjusted_close'])
        self.factor = market.groupby(level=1).apply(lambda x:
            ((x.loc[self.date, 'adjusted_close'] - x.loc[:, 'adjusted_close'].mean()) / 
            x.loc[:, 'adjusted_close'].mean()).values[0])
        self.factor.name = 'bias_1m'

class Davol(FactorBase):
    '''Stock Turnover 20 days / Turnover 120 days
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, with_group=True):
        super().__init__('davol', with_group)
        
    def calcuate_factor(self, date):
        def _cal(x):
            s = x.loc[last_short_date:date, 's_dq_freeturnover'].mean()
            l = x.loc[last_long_date:date, 's_dq_freeturnover'].mean()
            if l == 0:
                return np.nan
            return s / l
        # calculate factor
        last_long_date = last_n_trade_dates(date, 120)
        last_short_date = last_n_trade_dates(date, 20)
        date = str2time(date)
        last_long_date = str2time(last_long_date)
        last_short_date = str2time(last_short_date)
        market = derivative_indicator(last_long_date, date, 
            ['trade_dt', 's_info_windcode', 's_dq_freeturnover'])
        self.factor = market.groupby(level=1).apply(_cal)


if __name__ == "__main__":
    ar_loader = Ar('2013-01-31')
    print(ar_loader())