import numpy as np
from ..tools import PanelFrame


class PreProcessor(object):
    '''Data PreProcessor, using to preprocess data before passing panel data to calculation
    ======================================================================================

    This is a Class used to preprocess data before passing panel data to calculation.
    '''

    def __init__(self, panel: PanelFrame):
        self.panel = panel
    
    def deextreme(self, method: str = "mean_std", n: 'int | list' = 5) -> PanelFrame:
        """deextreme data
        ----------------

        method: str, must be one of these:
            'median': median way
            'fix_odd': fixed odd way
            'mean_std': mean_std way
        n: int or list, 
            when using median, n is the number of deviations from median
            when using fixed odd percentile, n must be one of those:
                1. the remaining percentiles for head and tail in a list form
                2. the n percentiles for head and tail in a float form
            when using mean std, ni is the number of deviations from mean

        return: PanelFrame, result
        """
        def _mean_std(data, n):
            mean = data.mean()
            std = data.std()
            data = data.copy()
            up = mean + n * std
            down = mean - n * std
            data = data.clip(down, up, axis=1)
            return data
        
        def _median_correct(data, n):
            ''''''
            md = data.median()
            mad = (data - md).abs().median()
            up = md + n * mad
            down = md - n * mad
            data = data.copy()
            data = data.clip(down, up, axis=1)
            return data
        
        def _fix_odd(data, n):
            if isinstance(n, (list, tuple)):
                data = data.copy()
                data = data.clip(data.quantile(n[0]), data.quantile(n[1]), axis=1)
                return data
            else:
                data = data.copy()
                data = data.clip(data.quantile(n / 2), data.quantile(1 - n / 2), axis=1)
                return data

        data = self.panel.groupby(level=0).apply(eval(f'_{method}'), n=n)
        return PanelFrame(dataframe=data)
        
    def standard(self, method: str = 'zscore'):
        """standardization ways
        -----------------

        method: str, standardization type must be one of these:
            'zscore': z-score standardization
            'weight': weight standardization
            'rank': rank standardization
        return: np.array, 标准化后的因子数据
        """
        def _zscore(data):
            mean = data.mean()
            std = data.std()
            data = (data - mean) / std
            return data
        
        data = self.panel.groupby(level=0).apply(eval(f'_{method}'))        
        return PanelFrame(dataframe=data)
    
    def missing_fill(self, method: str = 'fillzero'):
        """missing fill ways
        ---------------------

        data: np.array, raw data
        method: choices between ['drop', 'zero']
        return: np.array, missing data filled with 0
        """
        def _dropna(data):
            return data.dropna()

        def _fillzero(data):
            data = data.copy()
            data = data.fillna(0)
            return data
        
        def _fillmean(data):
            data = data.copy()
            data = data.fillna(data.mean())
            return data
        
        def _fillmedian(data):
            data = data.copy()
            data = data.fillna(data.median())
            return data

        data = self.panel.groupby(level=0).apply(eval(f'_{method}'))
        return PanelFrame(dataframe=data)


if __name__ == "__main__":
    import pandas as pd
    import matplotlib.pyplot as plt
    
    indicators = dict(zip(
        [f'indicator{i + 1}' for i in range(5)],
        [pd.DataFrame(np.random.rand(100, 5), index=pd.date_range('2020-01-01', periods=100), columns=list('abcde')) for _ in range(5)]
        ))
    panel = PanelFrame(indicators=indicators)

    processor = PreProcessor(panel=panel)
    print(processor.deextreme('median_correct', n=5))
    print(processor.deextreme('fix_odd', n=[0.1, 0.9]))
    print(processor.deextreme('fix_odd', n=0.1))
    print(processor.deextreme('mean_std', n=3))
    print(processor.standard('zscore'))
    print(processor.missing_fill('fillzero'))
    print(processor.missing_fill('dropna'))
    print(processor.missing_fill('fillmean'))
    print(processor.missing_fill('fillmedian'))
