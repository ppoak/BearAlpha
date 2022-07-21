from bearalpha import *


class Evaluator(Worker):

    def sharpe(
        self, 
        rf: 'int | float | Series' = 0.04, 
        period: 'int | str' = 'a'
    ) -> 'DataFrame | Series': ...