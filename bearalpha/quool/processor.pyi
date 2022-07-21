from bearalpha import *


class Converter(Worker):

    def price2ret(
        self, 
        period: 'str | int', 
        open_col: str = 'close', 
        close_col: str = 'close', 
        method: str = 'algret',
        lag: int = 1,
    ) -> 'DataFrame | Series': ...

    def cum2diff(
        self,
        grouper = None, 
        period: int = 1, 
        axis: int = 0, 
        keep: bool = True
    ) -> 'DataFrame | Series': ...

    def dummy2category(
        self, 
        dummy_col: list = None, 
        name: str = 'group'
    ) -> 'DataFrame | Series': ...

    def logret2algret(self) -> 'DataFrame | Series': ...

    def resample(self, rule: str, **kwargs) -> 'DataFrame | Series': ...


class PreProcessor(Worker):

    def standarize(
        self, 
        method: str = 'zscore', 
        grouper = None
    )  -> 'DataFrame | Series': ...

    def deextreme(
        self,
        method = 'mad', 
        grouper = None, 
        n = None
    ) -> 'DataFrame | Series': ...

    def fillna(
        self, 
        method = 'pad_zero', 
        grouper = None
    ) -> 'DataFrame | Series': ...