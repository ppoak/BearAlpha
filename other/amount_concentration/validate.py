# --
import pandas as pd
import matplotlib.pyplot as plt
from utils.get_data import *

# ---
start = '2010-01-01'
end = '2022-02-01'

# --- 
concentration = pd.read_csv('other/amount_concentration/result/result.csv', 
    index_col=0, parse_dates=[0])
concentration = concentration.loc[start:end]
concentration.plot()
plt.savefig("other/amount_concentration/images/result20.png")

# ---
trade_dates_monthly = trade_date(start, end, freq='monthly')
big = index_market_daily('000043.SH', start, end, fields=['trade_dt', 's_dq_close'])
small = index_market_daily('000045.SH', start, end, fields=['trade_dt', 's_dq_close'])
big.set_index('trade_dt', inplace=True)
small.set_index('trade_dt', inplace=True)
big = big.loc[trade_dates_monthly]
small = small.loc[trade_dates_monthly]

# ---
bigrel = big / big.iloc[0]
smallrel = small / small.iloc[0]
rel = smallrel - bigrel

# ---
_, axes = plt.subplots(2, 1, figsize=(20, 16))
rel.plot(ax=axes[0])
concentration.plot(ax=axes[1], color='green')
axes[1].hlines(0.45, xmin=axes[1].axes.get_xlim()[0], 
    xmax=axes[1].axes.get_xlim()[1], color='red', linestyle='--')



# --
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from utils.get_data import *

start = '2010-01-01'
end = '2022-02-01'

concentration = pd.read_csv('other/amount_concentration/result/result.csv',
    index_col=0, parse_dates=[0])
concentration.rename(columns={'0': 'data'}, inplace=True)

concentration_ori = concentration.copy()

hs300 = index_market_daily('000300.SH', start, end, fields=['trade_dt', 's_dq_close'])
hs300.set_index('trade_dt', inplace=True)

# __
concentration['data_lag'] = concentration['data'].shift(1)
concentration['cross'] = concentration['data']
filter_1 = concentration['cross'] > 0.45
filter_2 = concentration['data_lag'] < 0.45
concentration.where(filter_1 & filter_2, inplace=True)
concentration =  concentration.dropna(how='any')
# --
concentration_list = concentration.index.tolist()
later = concentration_list[0]
trade_dates_daily = trade_date(start, end, freq='daily')
_start = list()
_end = list()
for date in concentration_list:
    if date < later:
        continue
    # 观察 突破阈值后 30天 hs300指数的变化
    later = date + datetime.timedelta(days=30)
    while later not in trade_dates_daily:
        later = later + datetime.timedelta(days=1)
    # 2014-12-05 00:00:00 (<class 'pandas._libs.tslibs.timestamps.Timestamp'>) -> datetime.date(2010, 1, 29) (datetime.date)
    _start.append(date.to_pydatetime().date())
    _end.append(later.to_pydatetime().date())
df_1 = hs300.loc[_start]
df_2 = hs300.loc[_end]
df_1['close_next_month'] = df_2['s_dq_close'].tolist()
df_1['rate'] = ((df_1['close_next_month'] - df_1['s_dq_close']) / df_1['s_dq_close']) * 100

x = range(0, len(df_1['rate'].index))
y = df_1['rate'].values

_, axes = plt.subplots(3, 1, figsize=(20, 16))
hs300.plot(ax=axes[0])
concentration_ori.plot(ax=axes[1], color='green')
axes[1].hlines(0.45, xmin=axes[1].axes.get_xlim()[0], 
    xmax=axes[1].axes.get_xlim()[1], color='red', linestyle='--')
axes[2].bar(x, y, width=0.5)
axes[2].hlines(0, xmin=axes[2].axes.get_xlim()[0], 
    xmax=axes[2].axes.get_xlim()[1], color='red', linestyle='--')
plt.sca(axes[2])
plt.xticks(x, [d.to_pydatetime().date() for d in df_1['rate'].index], rotation=45)
plt.ylabel('growth rate', fontsize=18)


