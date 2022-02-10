
#######################################
###########计算大小盘变化################
#######################################

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


#######################################
# 全部历史数据以结果0.45为界限，验证有效性###
#######################################

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


# %%

##################################
# 历史数据的前后5%为界限，验证有效性。##
##################################


import pandas as pd
import matplotlib.pyplot as plt
from utils.get_data import *
import datetime
from matplotlib import font_manager
my_font = font_manager.FontProperties(fname='/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf')

start = '2010-01-01'
end = '2022-02-01'

concentration = pd.read_csv('other/amount_concentration/result/result.csv',
    index_col=0, parse_dates=[0])
concentration.rename(columns={'0': 'data'}, inplace=True)
concentration_ori = concentration.copy()

trade_dates_daily = trade_date(start, end, freq='daily')


# 由于验证前需要有一定的历史数据，所以从2011年开始计算。

val_start = '2011-01-01'


# --
# 选出超过历史值95%和小于历史值5%的日期
s = pd.Series(trade_dates_daily)
end_date = datetime.datetime.strptime(end, '%Y-%m-%d').date()
below_low_date = list()
above_high_date = list()
low_list = list()
high_list = list()
val_date = list()

flag = False
cur_date = val_start
while cur_date <= end:
    cur_date = datetime.datetime.strptime(cur_date, '%Y-%m-%d').date()
    while cur_date not in trade_dates_daily:
        cur_date = cur_date + datetime.timedelta(days=1)
        if cur_date > end_date:
            flag = True
            break
    if flag:
        break
    past = s.where(s < cur_date).dropna(how='any')
    tmp_concentration = concentration.loc[past]
    tmp_concentration = tmp_concentration['data'].sort_values()
    low, high = tmp_concentration.quantile([0.05, 0.95])
    cur_value = concentration.loc[datetime.datetime.strftime(cur_date, '%Y-%m-%d')]
    if cur_value.to_numpy()[0] > high:
        above_high_date.append(cur_date)
    if cur_value.to_numpy()[0] < low:
        below_low_date.append(cur_date)
    low_list.append(low)
    high_list.append(high)
    val_date.append(cur_date)
    cur_date = cur_date + datetime.timedelta(days=1)
    cur_date = datetime.datetime.strftime(cur_date, '%Y-%m-%d')
    

# --
df_pct_var = pd.DataFrame({'date':val_date, '5%':low_list, '95%':high_list})
df_pct_var.set_index('date', inplace=True)

x = df_pct_var.index.to_numpy()
y1 = df_pct_var['5%'].to_numpy()
y2 = df_pct_var['95%'].to_numpy()

plt.figure(figsize=(20,8))
plt.plot(x, y1, label='5%')
plt.plot(x, y2, label='95%')
plt.xticks(fontsize=18)
plt.yticks(fontsize=18)
plt.grid()
plt.title('成交额集中度的变化', fontsize=20,fontproperties=my_font)
plt.legend(prop={'size':25})
plt.show()



# --

hold_period = 7


# 计算买卖日期
def cal_tradedate(signal_date: list,
               hold_period: int) -> pd.DataFrame:
    """
        signal_date: 触发信号的日期列表
        hold_period: 触发信号后持有的时长
        return: 买入日期和卖出日期的表

        出现信号为买入日期，并在买入后至少持有hold_period日。
        若在持有期间再次出现信号，则在hold_period日后继续持
        有hold_period日。
    """
    cal_df = pd.DataFrame({'buy_date':signal_date})
    cal_df['sell_date'] = cal_df['buy_date'] + datetime.timedelta(hold_period)

    for i in cal_df['sell_date'].index:
        while cal_df.iloc[i]['sell_date'] not in trade_dates_daily:
            cal_df.at[i, 'sell_date'] = cal_df.iloc[i]['sell_date'] + datetime.timedelta(days=1)
    cal_df['delete'] = False
    while True:
        for i, (sdate, edate) in enumerate(zip(cal_df['buy_date'], cal_df['sell_date'])):
            if cal_df.iloc[i]['delete'] == True:
                continue
            cal_df.loc[(cal_df['buy_date'] > sdate) & (cal_df['buy_date'] < edate), 'delete'] = True

        for i, (sdate, edate) in enumerate(zip(cal_df['buy_date'], cal_df['sell_date'])):
            if cal_df.iloc[i]['delete'] == True:
                continue
            tmp_df = cal_df[(cal_df['buy_date'] > sdate) & (cal_df['buy_date'] < edate)]
            if len(tmp_df) != 0:
                cal_df.at[i, 'sell_date'] = cal_df.at[i, 'sell_date'] + datetime.timedelta(hold_period)
            while cal_df.at[i, 'sell_date'] not in trade_dates_daily:
                cal_df.at[i, 'sell_date'] = cal_df.at[i, 'sell_date'] + datetime.timedelta(days=1)
        count = cal_df[cal_df['delete']] == True
        if len(count) == 0:
            break
        cal_df.drop(cal_df[cal_df['delete'] == True].index, inplace=True)
        cal_df.reset_index(drop=True, inplace=True)
    return cal_df.drop(columns=['delete'])

# --



def back_track(index: pd.Series,
               trade_plan: pd.DataFrame,
               init_worth: int ) -> list:
    trade_plan['buy_value'] = index.loc[trade_plan['buy_date'].apply(lambda x:x.strftime('%Y-%m-%d'))]['s_dq_close'].to_numpy()
    trade_plan['sell_value'] = index.loc[trade_plan['sell_date'].apply(lambda x:x.strftime('%Y-%m-%d'))]['s_dq_close'].to_numpy()
    trade_plan['gain_rate'] = (trade_plan['sell_value'] - trade_plan['buy_value']) / trade_plan['buy_value']
    tmp = trade_plan['gain_rate'].to_numpy()
    net_worth = list()
    net_worth.append(init_worth)
    for i in tmp:
        net_worth.append(net_worth[-1] + i)
    rm = net_worth.pop(0)
    trade_plan['net_worth'] = net_worth
    win_rate = len(trade_plan[trade_plan['gain_rate'] > 0]) / len(trade_plan)
    return [trade_plan, win_rate]




# --

def plot(cal_df: pd.DataFrame,
         win_rate: int,
         index: str,
         method: str):
    x = cal_df['sell_date'].apply(lambda x:x.strftime('%Y-%m-%d')).to_numpy()
    _x = range(0,len(cal_df))
    y_1 = cal_df['net_worth'].to_numpy()
    y_2 = cal_df['gain_rate'].to_numpy()
    fig, axes = plt.subplots(2, 1, figsize=(20, 16))

    axes[0].bar(x, y_2)
    plt.suptitle(f'{index}: use {method} as trigger, win rate: {win_rate:.2f}', size=30)
    plt.sca(axes[0])
    plt.xticks(_x, x, fontsize=12, rotation=45)
    plt.yticks(fontsize=18)
    plt.grid()

    axes[1].plot(x,y_1)
    plt.sca(axes[1])
    plt.xticks(_x, x, fontsize=12, rotation=45)
    plt.yticks(fontsize=18)
    plt.grid()

# 回测hs300收益
hs300 = index_market_daily('000300.SH', val_start, end, fields=['trade_dt', 's_dq_close'])
hs300.set_index('trade_dt', inplace=True)
cal_df = cal_tradedate(above_high_date, 7)
cal_df_low = cal_tradedate(below_low_date, 7)
init_worth = 1
cal_df, win_rate = back_track(hs300, cal_df, init_worth)
cal_df_low, win_rate_low = back_track(hs300, cal_df_low, init_worth)
plot(cal_df, win_rate, 'hs300', '95%')
plot(cal_df_low, win_rate_low, 'hs300', '5%')

# 回测上证指数
sz_index = index_market_daily('000001.SH', val_start, end, fields=['trade_dt', 's_dq_close'])
sz_index.set_index('trade_dt', inplace=True)
cal_df = cal_tradedate(above_high_date, 7)
cal_df_low = cal_tradedate(below_low_date, 7)
init_worth = 1
cal_df, win_rate = back_track(sz_index, cal_df, init_worth)
cal_df_low, win_rate_low = back_track(sz_index, cal_df_low, init_worth)
plot(cal_df, win_rate, 'sz_index', '95%')
plot(cal_df_low, win_rate_low, 'sz_index', '5%')




# %%
