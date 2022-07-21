from bearalpha import *


class Calculator(Worker):

    def rolling(
        self, 
        window: int, 
        func, 
        *args, 
        processes: int = 1,
        offset: int = 0, 
        interval: int = 1, 
        **kwargs
    ) -> 'DataFrame | Series': ...

    def group_apply(
        self, 
        grouper: ..., 
        func: ..., 
        *args, 
        processes: int = 4, 
        **kwargs
    ) -> 'Series | DataFrame': ...