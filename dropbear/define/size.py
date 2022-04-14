import numpy as np
from .base import FactorBase
from ..utils import *
from utils import *


class AcmktCap(FactorBase):
    '''log(total circulating A shares * previous close price)
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, with_group=True):
        super().__init__('acmktcap', with_group)
    
    def calcuate_factor(self, date):  
        # calculate factor
        free_shares = derivative_indicator(date, date, ['s_info_windcode', 's_dq_mv'])
        self.factor = np.log(free_shares)
        self.factor.name = self.name

class AmktCap(FactorBase):
    '''log(total circulating A shares * previous close price)
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, with_group=True):
        super().__init__('amktcap', with_group)
    
    def calcuate_factor(self, date):  
        free_shares = derivative_indicator(date, date, ['s_info_windcode', 's_val_mv'])
        factor = np.log(free_shares)
        factor.name = self.name

class MarketCap(FactorBase):
    '''log(total shares * previous close price)
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    def __init__(self, date, with_group=True):
        super().__init__('marketcap', date, with_group)

    def calcuate_factor(self, date):
        total_size = derivative_indicator(date, date,
            ['s_info_windcode', 'tot_shr_today', 's_dq_close_today'])
        self.factor = np.log(total_size.loc[:, 'tot_shr_today'] * total_size.loc[:, 's_dq_close_today'])
        self.factor = self.name


if __name__ == '__main__':
    print(AcmktCap('2021-01-05')())