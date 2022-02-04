import datetime
import matplotlib.pyplot as plt
from utils.get_data import *
from utils.io import *


def amount_concentration(date: Union[datetime.datetime, datetime.date, str]) -> float:
    market_data = market_daily(date, date, fields=['code', 'amount']).sort_values(by='amount', ascending=False)
    total_stock = market_data.shape[0]
    top5pct = int(total_stock * 0.05)
    return market_data.iloc[:top5pct].amount.sum() / market_data.amount.sum()

if __name__ == "__main__":
    trade_dates = trade_date('2010-01-01', '2022-01-30', freq='monthly')
    result = pd.Series(dtype=float)
    with progress:
        for date in progress.track(trade_dates, description='calculating ... '):
            result[date] = amount_concentration(date)

    _, ax = plt.subplots(1, 1, figsize=(20, 8))
    result.plot(ax=ax)
    result.name = 'result'
    ax.set_title('Amount Concentration')
    ax.set_xlabel('date')
    ax.set_ylabel('amount percentage')
    ax.hlines(0.50, xmin=ax.axes.get_xlim()[0], xmax=ax.axes.get_xlim()[1], color='red', linestyle='--')
    plt.savefig('other/amount_concentration/images/result.png')
    result.to_csv('other/amount_concentration/result/result.csv')
    print(result)
