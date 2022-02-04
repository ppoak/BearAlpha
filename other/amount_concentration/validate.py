# ---
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
