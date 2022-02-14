from utils import *
from factor.define import *
from factor.utils.analyze import *
from factor.utils.prepare import *
from factor.utils.visualize import *
from factor.utils.config import FACTORS


class Factor():
    
    def __init__(self) -> None:
        self.today = datetime.date.today()
        if datetime.datetime.now().hour <= 20:
            self.today -= datetime.timedelta(days=1)
        self.factors = FACTORS
    
    def get(self, factor: str, date: str) -> Union[pd.DataFrame, pd.Series]:
        if factor not in self.factors.keys():
            raise ValueError (f'{factor} is not defined!')
        return self.factors[factor]["dataloader"](date)


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

    def update(self) -> None:
        pass

class FactorTester(Factor):

    def __init__(self) -> None:
        super().__init__()

    def get(self, factors: Union[str, list], start: str, 
            end: str, freq: str = 'monthly',
            forward_period: Union[list, int] = 20) -> pd.DataFrame:
        dates = trade_date(start, end, freq=freq)
        self.data = factor_datas_and_forward_returns(factors, dates, forward_period)
        return self.data
    
    def analyze(self, path_prefix: str, quantiles: int = 5) -> dict:
        self.reg_result = regression(self.data)
        self.ic_result = ic(self.data)
        self.layer_result = layering(self.data, quantiles)
        self.reg_result.to_excel(path_prefix + '_regression.xlsx')
        self.ic_result.to_excel(path_prefix + '_ic.xlsx')
        self.layer_result.to_excel(path_prefix + '_layering.xlsx')
        return {
            "regression result": self.reg_result,
            "ic result": self.ic_result,
            "layeringresult": self.layer_result
        }
    
    def visualize(self, path_prefix: str) -> None:
        regression_plot(self.reg_result, path_prefix + '_regression.png')
        ic_plot(self.ic_result, path_prefix + '_ic.png')
        layering_plot(self.layer_result, path_prefix + '_layering.png')

if __name__ == "__main__":
    tester = FactorTester()
    tester.get(['return_1m', 'volatility_1m', 'turnover_1m', 'ar'],
        '2013-01-01', '2014-01-01', 'monthly', 20)
    tester.analyze('factor/result/test')
    tester.visualize('factor/result/test')