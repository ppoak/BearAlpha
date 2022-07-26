import pandas as pd
import numpy as np
from ..core import *
from ..tools import *


class CalculatorError(FrameWorkError):
    pass


@pd.api.extensions.register_dataframe_accessor("calculator")
@pd.api.extensions.register_series_accessor("calculator")
class Calculator(Worker):

    class _DatetimeLevel0Indexer(pd.api.indexers.BaseIndexer):
        
        def get_window_bounds(self, num_values: int = 0, min_periods: int = None,
            center: str = None, closed: str = None):
            # since _DatetimeLevelIndexer is inside Calculator,
            # we can assume that the data passed in is conformed to
            # the general rules of Worker and sorted by datetime
            dates = self.index.levels[0]
            dates_values = self.index.get_level_values(0)
            dates_values_r = dates_values[::-1]

            if self.window_size < 1:
                raise CalculatorError('internal', "window_size must be greater than 0")
            if self.window_size > dates.size:
                raise CalculatorError('internal', "window_size must be less than or equal to the number of dates")

            start_dates_dict, end_dates_dict = {}, {}
            for date in dates:
                start_dates_dict[date] = dates_values.tolist().index(date)
                end_dates_dict[date] = num_values - dates_values_r.tolist().index(date)
            end = dates_values.map(end_dates_dict).to_numpy(dtype='int64')
            end_pos = dates_values.tolist().index(dates[self.window_size - 1])
            end[:end_pos] = 0
            start = np.array(list(map(lambda x: start_dates_dict[
                dates[dates.tolist().index(dates_values[x - 1]) - self.window_size + 1]
                ] if x != 0 else 0, end)), dtype='int64')

            return start, end
    
    def _rolling(self, window: int, min_periods: int = None, center: bool = False,
        win_type: str = None, on: str = None, axis: 'int | str' = 0, 
        closed: bool = None, method: str = 'single') -> 'pd.core.window.Rolling | pd.core.window.Window':
        '''Dummy rolling function for multi-index data with level0 datetime
        -------------------------------------------------------------------

        window: int, the rolling window length
        min_periods: int, min length of the rolling window
        center: bool, whether to centralize label
        win_type: str, window type
        on: str, column to roll on
        axis: int | str, axis to roll on
        closed: bool, whether to include the last point
        method: str, rolling method
        '''
        if self.type_ == Worker.PN:
            indexer = self._DatetimeLevel0Indexer(window_size=window, index=self.data.index)
            return self.data.rolling(indexer, min_periods, center, win_type, on, axis, closed, method)
        else:
            return self.data.rolling(window, min_periods, center, win_type, on, axis, closed, method)
    
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
        processes: int = 4, 
        **kwargs
    ) -> 'pd.Series | pd.DataFrame':
        '''multi-process apply a function to each group
        ----------------------------------------------

        grouper: the grouper applied in func,
        func: the function applied to each group,
        processes: the number of processes used, default 4
        return: the result of func applied to each group
        '''
        groups = self.data.groupby(grouper)
        results = self.__split_job(list(groups), processes, func, *args, **kwargs)
        data = []
        for index, result in results.items():
            if isinstance(result, (pd.Series, pd.DataFrame)):
                result.index = pd.MultiIndex.from_product([[index], result.index])
            else:
                result = pd.Series([result], index=[index])
            data.append(result)
        return pd.concat(data).sort_index()


if __name__ == "__main__":
    import time
    def s(data, n):
        time.sleep(n)
        return data.value.sum()
        
    data = pd.DataFrame(np.random.rand(8), index=pd.MultiIndex.from_arrays(
        [pd.to_datetime(['20200101', '20200101', '20200102', '20200103', '20200103', '20200103', '20200104', '20200104']),
        ['A', 'B', 'A', 'A', 'B', 'C', 'A', 'B']], names=['datetime', 'asset']), 
        columns=['value'])

    start_time = time.time()
    print(data.groupby('asset').apply(s, 1))
    end_time = time.time()
    print(f'time: {end_time - start_time:.2f}')
    start_time = time.time()
    print(data.calculator.group_apply('asset', s, 1))
    end_time = time.time()
    print(f'time: {end_time - start_time:.2f}')

    start_time = time.time()
    print(data.calculator.rolling(window=2, func=s, n=1, processes=1))
    end_time = time.time()
    print(f'time: {end_time - start_time:.2f}')
    start_time = time.time()
    print(data.calculator.rolling(window=2, func=s, n=1, processes=5))
    end_time = time.time()
    print(f'time: {end_time - start_time:.2f}')
