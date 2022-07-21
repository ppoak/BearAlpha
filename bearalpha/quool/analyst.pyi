from typing import Any
from bearalpha import *

class Regressor(Worker):
    def __init__(self) -> DataFrame: ...

    def ols(
        self,
        y: Series, 
        intercept: bool = True,
        backend: str = 'statsmodels',
        **kwargs,
    ) -> 'Series | Any': ...

    def logistics(
        self,
        y: Series, 
        intercept: bool = True,
        backend: str = 'statsmodels',
        **kwargs,
    ) -> 'Series | Any': ...

    def wls(
        self, 
        y: Series,
        weights: Series,
        intercept: bool = True,
        backend: str = 'statsmodels',
        **kwargs
    ) -> 'Series | Any': ...


class Decompositer(Worker):

    def pca(
        self, 
        ncomp: int, 
        backend: str = 'statsmodels', **kwargs
    ) -> 'Series | Any': ...

    
class Describer(Worker):

    def corr(
        self, 
        other: Series = None, 
        method: str = 'spearman', 
        tvalue = False,
    ) -> 'Series | DataFrame': ...

    def ic(
        self, 
        forward: Series = None, 
        grouper = None, 
        method: str = 'spearman'
    ) -> 'Series | DataFrame': ...


class Tester(Worker):

    def sigtest(
        self, 
        h0: 'float | Series' = 0
    ) -> 'DataFrame | Series': ...
