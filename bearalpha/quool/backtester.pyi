import backtrader as bt
from bearalpha import *


class Relocator(quool.base.Worker):

    def profit(
        self, 
        ret: Series, 
        portfolio: Series = None,
    ) -> Series:
        """calculate profit from weight and forward
        ---------------------------------------------

        ret: pd.Series, the return data in either PN series or TS frame form
        portfolio: pd.Series, the portfolio tag marked by a series, 
            only available when passing a PN
        """

    def networth(
        self, 
        price: 'Series | DataFrame',
    ) -> Series:
        """Calculate the networth curve using normal price data
        --------------------------------------------------------

        price: pd.Series or pd.DataFrame, the price data either in
            MultiIndex form or the TS Matrix form
        return: pd.Series, the networth curve
        """

    def turnover(self, side: str = 'both') -> Series:
        """calculate turnover
        ---------------------

        side: str, choice between "buy", "short" or "both"
        """


class BackTrader(quool.base.Worker):

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
        spt: int, stock per trade, defining the least stocks in on trade
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
        spt: int, stock per trade, defining the least stocks in on trade
        ratio: float, retention ration for cash, incase of failure in order
        cash: int, initial cash
        indicators: bt.Indicator or list, a indicator or a list of them
        analyzers: bt.Analyzer or list, an analyzer or a list of them
        observers: bt.Observer or list, a observer or a list of them
        coc: bool, to set whether cheat on close
        image_path: str, path to save backtest image
        data_path: str, path to save backtest data
        show: bool, whether to show the result
        """


class Factester(quool.base.Worker):

    def analyze(
        self,
        price: 'Series | DataFrame',
        marketcap: 'Series | DataFrame' = None,
        grouper: 'Series | DataFrame | dict' = None, 
        benchmark: Series = None,
        periods: 'list | int' = [5, 10, 15], 
        q: int = 5, 
        commission: float = 0.001, 
        commission_type: str = 'both', 
        plot_period: 'int | str' = -1, 
        data_path: str = None, 
        image_path: str = None, 
        show: bool = True
    ): ...