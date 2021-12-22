import pandas as pd
import numpy as np
from utils import get_data, treatment
from rqdatac import get_previous_trading_date, get_price

def smooth_momentum_21(date: str,
                       n: int = 5,
                       deextreme_type: str = 'median',
                       standard_type: str = 'zscore') -> pd.Series:
    '''获取给定日期的21日平滑动量
    --------------------------
    
    传统动量存在着一个严重的问题，如果一段给定时间内的收益率对于两只股票来说是完全相同的，
    那么他们的动量将是相同的。但是他们形成这种相同收益率的路径可能是不同的。因此平滑动量
    考虑了形成路径的问题，使得动量的计算更为稳健
    
    Parameters:
    -------------
    
    date: str, 给定的日期
    n: int, 用于去极值的参数，详细内容见treatment.deextreme
    deextreme_type: str, 用于去极值的方法
    standard_type: str, 用于标准化的方法
    
    Return:
    -------------
    
    返回一个序列，包含标准化后的因子值
    '''
    # 获取可投股票池
    stock_pool = get_data.get_stock_pool(date)
    codes = stock_pool.order_book_id.tolist()
    # 获取需要的交易日及对应数据
    last_date = get_previous_trading_date(date, 21)
    close = get_price(codes, start_date=last_date, end_date=date, fields='close').close
    # 计算时段的收益率和总收益率
    period_ret = close.groupby('order_book_id').apply(lambda x: x.iloc[-1] / x.iloc[0] - 1)
    total_ret = close.groupby('order_book_id').apply(lambda x: np.abs(x / x.shift(1) - 1).sum())
    # 计算二者比值得到因子
    factor = period_ret / total_ret
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    # 去极值，标准化
    factor = treatment.deextreme(factor, deextreme_type=deextreme_type)
    factor = treatment.standard(factor, standard_type=standard_type)
    return factor

