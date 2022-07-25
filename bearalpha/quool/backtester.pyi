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
    ) -> None: 
        """Run a strategy using backtrader backend
        -----------------------------------------
        
        strategy: bt.Strategy
        cash: int, initial cash
        indicators: bt.Indicator or list, a indicator or a list of them
        analyzers: bt.Analyzer or list, an analyzer or a list of them
        observers: bt.Observer or list, a observer or a list of them
        coc: bool, to set whether cheat on close
        image_path: str, path to save backtest image
        data_path: str, path to save backtest data
        show: bool, whether to show the result
        """


    def relocate(
        self,
        portfolio: 'DataFrame | Series' = None,
        cash: float = 1000000,
        analyzers: 'bt.Analyzer | list' = None,
        observers: 'bt.Observer | list' = None,
        coc: bool = False,
        image_path: str = None,
        data_path: str = None,
        show: bool = True,
    ):
        """Test directly from dataframe position information
        -----------------------------------------
        
        portfolio: pd.DataFrame or pd.Series, position information
        cash: int, initial cash
        indicators: bt.Indicator or list, a indicator or a list of them
        analyzers: bt.Analyzer or list, an analyzer or a list of them
        observers: bt.Observer or list, a observer or a list of them
        coc: bool, to set whether cheat on close
        image_path: str, path to save backtest image
        data_path: str, path to save backtest data
        show: bool, whether to show the result
        """

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
