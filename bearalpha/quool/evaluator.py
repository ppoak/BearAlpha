import pandas as pd
import numpy as np
from .base import *
from ..tools import *


class EvaluaterError(FrameWorkError):
    pass


@pd.api.extensions.register_dataframe_accessor("evaluator")
@pd.api.extensions.register_series_accessor("evaluator")
class Evaluator(Worker):
    """Evaluater is a evaluate staff to analyze the 
    performance of given portfolio net value curve."""

    def _valid(self, data: 'pd.Series | pd.DataFrame'):
        if self.ists(data):
            return data.copy()
        if self.isseries(data) and self.ispanel(data):
            return data.unstack()
        else:
            raise EvaluaterError('Evaluator', 'Your have to pass a panel series or time series')

    def sharpe(
        self, 
        rf: 'int | float | pd.Series' = 0.04, 
        period: 'int | str' = 'a'
    ):
        """To Calculate sharpe ratio for the net value curve
        -----------------------------------------------------
        
        rf: int, float or pd.Series, risk free rate, default to 4%,
        period: freqstr or dateoffset, the resample or rolling period
        """
        netcurve = self._valid(self.data)
        profit = netcurve.pct_change()
        if isinstance(period, int):
            return (profit.apply(lambda x: x - rf)).rolling(
                period).mean() / profit.rolling(period).std()
        else:
            return (profit.apply(lambda x: x - rf)).resample(
                period).mean() / profit.resample(period).std()


if __name__ == "__main__":
    data = pd.Series(np.random.rand(100), index=pd.MultiIndex.from_product(
        [pd.date_range('20200101', periods=20, freq='3d'), list('abcde')]))
    datacum = (data + 1).groupby(level=1).cumprod()
    print(datacum.evaluator.sharpe(period=5))
