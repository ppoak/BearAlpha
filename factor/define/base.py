import pandas as pd
import numpy as np
from utils import *


class FactorBase(object):
    '''Base Class for all factors to be defined
    ============================================

    There are five steps to define a factor:

        1. Define the class inheriting FactorBase
        
        2. rewrite `basic_info` according to your needs, this
            method provides basic information like stocks
            in stock pool and corresponding industry
        
        3. rewrite `calcuate_factor` according to your needs,
            this method can provide the factor value in a series form
            
        4. rewrite `factor_process` according to your needs,
            the process inculdes standardization, deextreme, missing fill

        5. rewrite `factor_modify` according to your needs,
            this method adjusted the index into multiindex form and rename
    '''
    def __init__(self, name, with_group=True):
        self.name = name
        self.with_group = with_group

    def basic_info(self, date):
        self.stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
        self.industry = plate_info(date, date, ['code', 'zxname_level1'])\
            .rename(columns={'zxname_level1': 'group'})

    def calcuate_factor(self, date):
        self.factor = pd.Series(dtype='float32')
        self.factor.name = self.name
        pass

    def factor_process(self):
        self.factor = pd.concat([self.factor, self.industry], axis=1)
        self.factor[self.name] = self.factor.groupby('group').apply(
            lambda x: standard(x.loc[:, self.name])).droplevel(0).sort_index()
        self.factor[self.name] = self.factor.groupby('group').apply(
            lambda x: deextreme(x.loc[:, self.name], n=3)).droplevel(0).sort_index()
        self.factor[self.name] = missing_fill(self.factor[self.name])
        self.factor = self.factor.loc[self.stocks]
    
    def factor_modify(self):
        self.factor.index = pd.MultiIndex.from_product([[self.date], self.factor.index])
        self.factor.index.names = ["date", "asset"]
        if not self.with_group:
            self.factor = self.factor.loc[:, self.name]
    
    def __call__(self, date) -> Any:
        self.basic_info()
        self.calcuate_factor(date)
        self.factor_process()
        self.factor_modify()
        return self.factor
