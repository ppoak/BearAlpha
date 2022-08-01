from bearalpha.core import *
from bearalpha.quool import *


class Evaluator(Worker):

    def sharpe(
        self, 
        rf: 'int | float | Series' = 0.04, 
        period: 'int | str' = 'a'
    ) -> 'DataFrame | Series':
        """To Calculate sharpe ratio for the net value curve
        -----------------------------------------------------
        
        rf: int, float or pd.Series, risk free rate, default to 4%,
        period: freqstr or dateoffset, the resample or rolling period
        """
