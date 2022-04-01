from curses import window
import os
import pandas as pd
import numpy as np
from rich.progress import track
from functools import lru_cache
from scipy.io import loadmat


@lru_cache(maxsize=None, typed=False)
def intraday_index(date: pd.Timestamp, freq='1min'):
    '''生成日内的交易数据的索引

    date: datetime格式日期
    return: 返回给定日期的分钟级别的交易时间时间戳类型索引
    '''
    morning = pd.date_range(f'{date.date()} 9:30:00', f'{date.date()} 11:30:00', freq=freq)
    afternoon = pd.date_range(f'{date.date()} 13:00:00', f'{date.date()} 15:00:00', freq=freq)
    return morning.tolist() + afternoon.tolist()

def calc_overnight_ret(data: pd.DataFrame):
    open_price = data['open'].resample('d').first()
    close_price = data['close'].resample('d').last()
    overnight_ret = np.log(open_price / close_price.shift(1))
    overnight_ret.name = 'overnight'
    return overnight_ret.dropna().astype('float32')

def calc_intraday_ret(data: pd.DataFrame):
    intraday_ret = data['close'].resample('d').apply(lambda x: np.log(x.values / x.shift(1).values))
    intraday_ret = intraday_ret.loc[intraday_ret.str.len() >= 1]
    intraday_ret.name = 'intraday'
    return intraday_ret

def gentle_or_extreme(data: pd.DataFrame, kind: str, deviation: float):
    data = data.explode().dropna()
    md = np.median(data)
    mad = np.median(np.abs(data - md))
    mad_e = 1.4826 * mad
    if kind == 'gentle':
        ret = data.loc[np.abs(data - md) < deviation * mad_e]
    else:
        ret = data.loc[np.abs(data - md) >= deviation * mad_e]
    return ret.sum()

def calc_gentle_ret(data: pd.DataFrame, window_size: int, deviation: float):
    intraday_ret = calc_intraday_ret(data)
    gentle_ret = pd.Series(dtype='float32')
    for i in range(window_size - 1, len(intraday_ret), window_size):
        gentle_ret.loc[intraday_ret.index[i]] = gentle_or_extreme(
            intraday_ret.iloc[i - window_size + 1:i + 1], kind='gentle', deviation=deviation)
    gentle_ret.name = f'gentle'
    return gentle_ret

def calc_extreme_ret(data: pd.DataFrame, window_size: int, deviation: float):
    intraday_ret = calc_intraday_ret(data)
    extreme_ret = pd.Series(dtype='float32')
    for i in range(window_size - 1, len(intraday_ret), window_size):
        extreme_ret.loc[intraday_ret.index[i]] = gentle_or_extreme(
            intraday_ret.iloc[i - window_size + 1:i + 1], kind='extreme', deviation=deviation)
    extreme_ret.name = f'extreme'
    return extreme_ret

def calc_ret_from_mat(code, adj_factor, window_size = 1, deviation = 1.96):
    '''将mat文件转换为csv文件

    code: 股票代码
    adj_factor: 复权因子, 这里的复权因子为code对应股票的Series, 索引为时间类型，值为复权因子
    '''
    mat_file = f'industrial_momentum_and_reverse/data/mat/UnAdjstedStockMinute_{code}.mat'
    mat = loadmat(mat_file)
    mat_data = mat[f'UnAdjstedStockMinute_{code}']
    df_data = pd.DataFrame(mat_data, columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'amount'])
    df_data['datetime'] = df_data['datetime'].apply(int).apply(str)
    df_data = df_data.set_index('datetime')
    # 因为adjust factor数据是从2005-01-04开始的, 在此前的数据是无效的需要删除
    df_data.index = pd.to_datetime(df_data.index)
    df_data = df_data.loc['2005-01-04':]
    result = []
    for date in df_data.index.unique():
        idx = intraday_index(date)
        # 使用tmp_df构造一个空的DataFrame，因为可能存在某些时间段没有数据，这时候需要将其设置为nan
        tmp_df = pd.DataFrame(index=idx, columns=['open', 'high', 'low', 'close', 'volume', 'amount'])
        row = df_data.loc[date].shape[0]
        tmp_df.iloc[:row] = df_data.loc[date].values * adj_factor.loc[date]
        result.append(tmp_df)
    result = pd.concat(result).astype('float32')
    # 将result resample成五分钟频率的 (resample后是column双索引, 且会有大量nan值)
    result = result.resample('5min').ohlc().loc[:, [('open', 'open'), ('high', 'high'),
        ('low', 'low'), ('close', 'close')]].droplevel(0, axis=1).dropna(how='all')
    overnight_ret = calc_overnight_ret(result)
    gentle_ret = calc_gentle_ret(result, window_size=window_size, deviation=deviation)
    extreme_ret = calc_extreme_ret(result, window_size=window_size, deviation=deviation)
    ret = pd.concat([overnight_ret, gentle_ret, extreme_ret], axis=1)
    return ret
    

if __name__ == '__main__':
    # codes = sorted(list(map(lambda x: x.split('_')[-1].split('.')[0], os.listdir('industrial_momentum_and_reverse/data/mat'))))
    # adj_factor = pd.read_csv('industrial_momentum_and_reverse/data/adjfactor.csv', index_col=0, parse_dates=[0]).dropna(how='all', axis=1)

    # for code in track(codes):
    #     code_columns = adj_factor.columns[adj_factor.columns.str.contains(code)]
    #     if len(code_columns) != 1:
    #         print(f"{code} not in adj_factor or more than one match")
    #         continue
    #     adj = adj_factor[code_columns[0]]
    #     ret = calc_ret_from_mat(code, adj)
    #     ret.to_csv(f'industrial_momentum_and_reverse/data/stock/{code}.csv')

    
    # 合并数据
    codes = os.listdir('industrial_momentum_and_reverse/data/stock')
    overnight_ret = []
    gentle_ret = []
    extreme_ret = []
    for code_file in track(codes):
        dt = pd.read_csv(f'industrial_momentum_and_reverse/data/stock/{code_file}', index_col=0, parse_dates=[0])
        overnight_ret.append(dt['overnight'])
        gentle_ret.append(dt['gentle_ret'])
        extreme_ret.append(dt['extreme_ret'])
    overnight_ret = pd.concat(overnight_ret).astype('float32')
    gentle_ret = pd.concat(gentle_ret).astype('float32')
    extreme_ret = pd.concat(extreme_ret).astype('float32')
    overnight_ret.to_csv('industrial_momentum_and_reverse/data/overnight_ret.csv')
    gentle_ret.to_csv('industrial_momentum_and_reverse/data/gentle_ret.csv')
    extreme_ret.to_csv('industrial_momentum_and_reverse/data/extreme_ret.csv')
