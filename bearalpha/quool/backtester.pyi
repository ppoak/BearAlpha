import datetime
import backtrader as bt
from bearalpha import *


class Relocator(Worker):

    def profit(
        self, 
        ret: Series, 
        portfolio: Series = None,
    ) -> Series: ...

    def networth(
        self, 
        price: 'Series | DataFrame',
    ) -> Series: ...

    def turnover(self, side: str = 'both') -> Series: ...


class BackTrader(Worker):

    def run(
        self, 
        strategy: Strategy = None, 
        cash: float = 1000000,
        indicators: 'Indicator | list' = None,
        analyzers: 'Analyzer | list' = None,
        observers: 'Observer | list' = None,
        coc: bool = False,
        image_path: str = None,
        data_path: str = None,
        show: bool = True,
    ) -> None: ...


class Strategy(bt.Strategy):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO') -> None: ...


class Analyzer(bt.Analyzer):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO') -> None: ...

class Observer(bt.Observer):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO') -> None: ...

class Indicator(bt.Indicator):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO') -> None: ...

class OrderTable(Analyzer):
    ...
