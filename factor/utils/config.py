from utils import *
from pandas import Series
from factor.define import *

FACTORS = {
    "return_1m": {
        "table": "technical",
        "dataloader": return_1m,
        },
    "return_3m": {
        "table": "technical",
        "dataloader": return_3m,
        },
    "return_12m": {
        "table": "technical",
        "dataloader": return_12m,
        },
    "turnover_1m": {
        "table": "technical",
        "dataloader": turnover_1m,
        },
    "turnover_3m": {
        "table": "technical",
        "dataloader": turnover_3m,
        },
    "volatility_1m": {
        "table": "technical",
        "dataloader": volatility_1m,
        },
    "volatility_3m": {
        "table": "technical",
        "dataloader": volatility_3m,
        },
    "volatility_12m": {
        "table": "technical",
        "dataloader": volatility_12m},
    "ar": {
        "table": "technical",
        "dataloader": ar,
        },
    "br": {
        "table": "technical",
        "dataloader": br,
        },
    "bias_1m": {
        "table": "technical",
        "dataloader": bias_1m,
        },
    "davol_1m": {
        "table": "technical",
        "dataloader": davol_1m,
        },
}

class FactorSaver(Factor):
    
    def __init__(self) -> None:
        super().__init__()

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
    factor = FactorSaver()
    print(factor.get('return_1m', '2019-01-04'))
