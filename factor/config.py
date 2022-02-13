from pandas import Series
from factor.define import *

class Factor():
    
    def __init__(self) -> None:
        self.today = datetime.date.today()
        if datetime.datetime.now().hour <= 20:
            self.today -= datetime.timedelta(days=1)
        self.technical = technical
        self.size = size
        self.factors = [
            'return_1m',
            'return_3m',
            'return_12m',
            'turnover_1m',
            'turnover_3m',
            'volatility_1m',
            'volatility_3m',
            'volatility_12m',
            'ar',
            'br',
            'bias_1m',
            'davol_1m',
        ]
        self.dataloader = dict(zip(self.factors, list(map(eval, self.factors))))
        self.tables = {
            'return_1m': "technical",
            'return_3m': "technical",
            'return_12m': "technical",
            'turnover_1m': "technical",
            'turnover_3m': "technical",
            'volatility_1m': "technical",
            'volatility_3m': "technical",
            'volatility_12m': "technical",
            'ar': "technical",
            'br': "technical",
            'bias_1m': "technical",
            'davol_1m': "technical",
        }
    
    def _generate_args(self, start: str, end: str) -> list:
        args = trade_date(start, end)
        return args
    
    def _check_latest(self, table: str, factor: str) -> datetime.date:
        latest_date = pd.read_sql(f'select max(trade_date) from {table}' + 
            f'where factor_name = "{factor}"', factor_database).iloc[0, 0]
        return latest_date

    def _save_by_args(self, table: str, factor: str, args: list) -> None:
        for date in args:
            data = self.get(factor, date)
            to_sql(table, factor_database, data)
    
    def get(self, factor: str, date: str) -> Union[pd.DataFrame, pd.Series]:
        if factor not in self.factors:
            raise ValueError (f'{factor} is not defined!')
        return self.dataloader[factor](date)
    
    def save_single_table(self, table: str, factor: str, start: str, end: str) -> None:
        args = self._generate_args(start, end)
        self._save_by_args(table, factor, args)

    def update_single_table(self, table: str, factor: str) -> None:
        latest_date = self._check_latest(table, factor)
        if self.today == latest_date:
            console.print(f'table {table} is current up to date')
            return
        args = self._generate_args(next_n_trade_dates(latest_date, 1), self.today)
        self._save_by_args(table, factor, args)
    
    def save(self, start: str, end:str) -> None:
        for factor, table in self.tables.items():
            self.save_single_table(table, factor, start, end)
    

if __name__ == "__main__":
    factor = Factor()
    print(factor.get('return_1m', '2019-01-04'))
