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
    ) -> 'DataFrame | Series':
        '''Provide rolling window func apply for pandas dataframe
        ----------------------------------------------------------

        window: int, the rolling window length
        func: unit calculation function
        args: arguments apply to func
        offset: int, the offset of the index, default 0 is the latest time
        kwargs: the keyword argument applied in func
        '''


    def group_apply(
        self, 
        grouper: ..., 
        func: ..., 
        *args, 
        processes: int = 4, 
        **kwargs
    ) -> 'Series | DataFrame':
        '''multi-process apply a function to each group
        ----------------------------------------------

        grouper: the grouper applied in func,
        func: the function applied to each group,
        processes: the number of processes used, default 4
        return: the result of func applied to each group
        '''
