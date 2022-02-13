from .technical import *
from .size import *

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
    
    def get(self, factor: str, date: str) -> Union[pd.DataFrame, pd.Series]:
        if factor not in self.factors:
            raise ValueError (f'{factor} is not defined!')
        return self.dataloader[factor](date)
