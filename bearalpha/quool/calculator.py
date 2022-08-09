import pandas as pd
import numpy as np
from .base import *
from ..tools import *


class CalculatorError(FrameWorkError):
    pass


@pd.api.extensions.register_dataframe_accessor("calculator")
@pd.api.extensions.register_series_accessor("calculator")
class Calculator(Worker):

    @staticmethod
    def __split_job(data: 'list[tuple[any, pd.DataFrame | pd.Series]]', processes: int, 
        func: ..., *args, **kwargs) -> 'dict[any, pd.DataFrame | pd.Series]':
        '''Split job into processes
        ------------------------------

        data: list[tuple[any, pd.DataFrame | pd.Series]], the data to split, with (index, data) form
        processes: int, the number of processes used
        func: the function applied to each group,
        args: the arguments of func
        kwargs: the keyword arguments of func
        '''
        import multiprocessing
        context = multiprocessing.get_context('fork')
        pool = context.Pool(processes=processes)
        results = {}
        for idx, dat in data:
            results[idx] = pool.apply_async(func, args=(dat, ) + args, kwds=kwargs)
        pool.close()
        pool.join()
        for idx, res in results.items():
            results[idx] = res.get()
        return results

    def rolling(
        self, 
        window: int, 
        func, 
        *args, 
        processes: int = 1,
        offset: int = 0, 
        interval: int = 1, 
        **kwargs
    ):
        '''Provide rolling window func apply for pandas dataframe
        ----------------------------------------------------------

        window: int, the rolling window length
        func: unit calculation function
        args: arguments apply to func
        offset: int, the offset of the index, default 0 is the latest time
        kwargs: the keyword argument applied in func
        '''
        # in case of unsorted level and used level
        data = self.data.sort_index().copy()
        data.index = data.index.remove_unused_levels()

        if self.type_ == Worker.TSSR or self.type_ == Worker.TSFR:
            datetime_index = data.index
        elif self.type_ == Worker.PNFR or self.type_ == Worker.PNSR:
            datetime_index = data.index.levels[0]
        else:
            raise TypeError('rolling only support for panel or time series data')
        
        window_datas = []
        for i in range(window - 1, datetime_index.size, interval):
            window_data = data.loc[datetime_index[i - window + 1]:datetime_index[i]].copy()
            window_data.index = window_data.index.remove_unused_levels()
            window_datas.append((datetime_index[i - offset], window_data))

        if processes > 1:
            results = self.__split_job(window_datas, processes, func, *args, **kwargs)
        else:
            results = {}
            for idx, dt in window_datas:
                results[idx] = func(dt, *args, **kwargs)
            
        result_data = []
        for idx, res in results.items():
            if isinstance(res, (pd.DataFrame, pd.Series)):
                if isinstance(res.index, pd.MultiIndex) \
                    and len(res.index.levshape) >= 2:
                    raise CalculatorError('rolling', 'the result of func must be a single indexed')
                else:
                    res.index = pd.MultiIndex.from_product([[idx], res.index])
            else:
                res = pd.DataFrame([res], index=[idx])
            
            result_data.append(res)
            
        result = pd.concat(result_data)
        return result

    def group_apply(
        self, 
        grouper: ..., 
        func: ..., 
        *args, 
        **kwargs
    ) -> 'pd.Series | pd.DataFrame':
        '''multi-process apply a function to each group
        ----------------------------------------------

        grouper: the grouper applied in func,
        func: the function applied to each group,
        processes: the number of processes used, default 4
        return: the result of func applied to each group
        '''
        from pandarallel import pandarallel
        pandarallel.initialize()

        self.data.groupby(grouper).parallel_apply(func, *args, **kwargs)
