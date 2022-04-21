# ---
import pandasquant as pq
import numpy as np
from scipy.stats import norm

# ---
index = pq.read_excel('other/financial_ratio/index_list.xlsx')
all_index = index['from'].dropna().unique().tolist() + index['to'].dropna().unique().tolist()

income_index = set(filter(lambda x: x.startswith('income'), all_index))
balance_index = set(filter(lambda x: x.startswith('balance'), all_index))
cashflow_index = set(filter(lambda x: x.startswith('cashflow'), all_index))
income_col = list(map(lambda x: x.split('.')[1], income_index)) + ['c_statementType', 'c_reportPeriod', 'c_windCode']
balance_col = list(map(lambda x: x.split('.')[1], balance_index)) + ['c_statementType', 'c_reportPeriod', 'c_windCode']
cashflow_col = list(map(lambda x: x.split('.')[1], cashflow_index)) + ['c_statementType', 'c_reportPeriod', 'c_windCode']

# ---
balance = pd.read_csv('assets/data/financial_report/balancesheet.csv', 
    index_col=['c_reportPeriod', 'c_windCode'], parse_dates=True, usecols=balance_col).sort_index()
income = pd.read_csv('assets/data/financial_report/income.csv', 
    index_col=['c_reportPeriod', 'c_windCode'], parse_dates=True, usecols=income_col).sort_index()
cashflow = pd.read_csv('assets/data/financial_report/cashflow.csv', 
    index_col=['c_reportPeriod', 'c_windCode'], parse_dates=True, usecols=cashflow_col).sort_index()

# ----
balance = balance.loc[balance.c_statementType == 408001000]
income = income.loc[income.c_statementType == 408001000]
cashflow = cashflow.loc[cashflow.c_statementType == 408001000]

daterange = pd.date_range('20100101', '20220331', freq='Q')
balance = balance.loc[daterange]
income = income.loc[daterange, :]
cashflow = cashflow.loc[daterange, :]

# ---
stock_status = pd.read_csv('assets/data/stock/status/industry.csv', panel=True)
stock_status = stock_status.loc[(daterange, slice(None)), slice(None)]
stock_status.index.names = ['', '']

# ---

balance_indexes = balance.loc[:, ["c_windCode", "c_reportPeriod"] + list(banlance_indexes)].set_index(['c_reportPeriod', 'c_windCode'])
income_indexes = income.loc[:, ["c_windCode", "c_reportPeriod"] + list(income_indexes)].set_index(['c_reportPeriod', 'c_windCode'])
cashflow_indexes = cashflow.loc[:, ["c_windCode", "c_reportPeriod"] + list(cashflow_indexes)].set_index(['c_reportPeriod', 'c_windCode'])

# ---
income_indicator = income_indexes.groupby(lambda x: x[0].year).apply(lambda x: x.groupby(level=1).apply(db.cum2diff))
income_indicator.index.names = ['', '']

# ---
balance_indicator = balance_indexes
balance_indicator.index.names = ['', '']

# ---
cashflow_indicator = cashflow_indexes.preprocess.cum2diff(lambda x: x.year)
cashflow_indicator.index.names = ['', '']

# ---
# income_indicator_g = db.merge(income_indicator, stock_status, left_index=True, right_index=True)
# balance_indicator_g = db.merge(balance_indicator, stock_status, left_index=True, right_index=True)
# cashflow_indicator_g = db.merge(cashflow_indicator, stock_status, left_index=True, right_index=True)

# ---
indicators = []
for idx in index_list.dropna().index:
    from_var = eval(index_list.loc[idx, 'from'].split('.')[0] + "_indicator")
    to_var = eval(index_list.loc[idx, 'to'].split('.')[0] + "_indicator")
    from_name = index_list.loc[idx, 'from'].split('.')[1]
    to_name = index_list.loc[idx, 'to'].split('.')[1]
    indi = from_var[from_name] / to_var[to_name]
    indi.name = from_name + '/' + to_name
    indicators.append(indi)
indicators = pd.concat(indicators, axis=1)

# ---
result = pd.DataFrame(columns=stock_status['group'].dropna().unique().tolist())
for industry in stock_status['group'].dropna().unique():
    industry_index = stock_status.loc[stock_status['group'] == industry].index
    industry_index = indicators.index.intersection(industry_index)
    industry_data = indicators.loc[industry_index, :]
    for i, end in enumerate(industry_data.index.levels[0][3:]):
        timespan = industry_data.index.levels[0][i:i + 4]
        industry_data_span = industry_data.loc[timespan, :]
        mean = industry_data_span.mean(axis=0)
        std = industry_data_span.std(axis=0)
        industry_data_span_normalized = (industry_data_span - mean) / std
        score = np.nanmean(2 * norm.sf(industry_data_span_normalized) - 1)
        result.loc[end, industry] = score
        