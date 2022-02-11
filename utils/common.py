from utils.io import *
from utils.getdata import *

def last_n_trade_dates(date: Union[datetime.datetime, datetime.date, str],
                       n: int) -> list[datetime.date]:
    '''Get last n trading dates
    -------------------------------------
    
    date: str, datetime or date, the given date
    n: int, the number of dates since the given date
    return: list, a list of dates
    '''
    last_date = str2time(date) - datetime.timedelta(days=n * 9 + 1)
    data = pd.read_sql(f'select trade_date from trade_date_daily ' + \
        f'where trade_date >= "{last_date}" and trade_date <= "{date}"',
        stock_database)
    data = data.trade_date.sort_values().iloc[-n - 1:].tolist()
    return data

if __name__ == "__main__":
    print(last_n_trade_dates('2022-02-07', 5))