import pandas as pd
import matplotlib.pyplot as plt
from utils import *
from factor.utils.prepare import *
from factor.utils.analyze import *

def regression_plot(regress_result: pd.DataFrame, path: str = None) -> None:
    ''''''
    def _reg_plot(d, ax):
        ax.set_title(f'regression: {factor} - {period}', fontsize=20)
        ax.bar(d.index, d[f'coef_{period}'], color='b', width=8, label=f'coef_{period}')
        ax.hlines(0, d.index[0], d.index[-1], color='k', linestyle='-.')
        ax = ax.twinx()
        ax.plot(d.index, d[f't_{period}'], color='r', label=f't_{period}')
        ax.hlines(2, d.index[0], d.index[-1], color='g', linestyle='--')
        ax.hlines(-2, d.index[0], d.index[-1], color='g', linestyle='--')
        ax.legend(loc='best')
        
    factors = regress_result.index.levels[0]
    periods = set(regress_result.columns.map(lambda x: x.split('_')[-1]))
    
    _, axes = plt.subplots(len(factors), len(periods), 
        figsize=(len(periods) * 17, len(factors) * 10))
    if len(factors) == 1 and len(periods) == 1:
        axes = np.array([[axes]])
    elif len(factors) == 1 and len(periods) > 1:
        axes = axes.reshape(1, -1)
    elif len(factors) > 1 and len(periods) == 1:
        axes = axes.reshape(-1, 1)

    for i, factor in enumerate(factors):
        for j, period in enumerate(periods):
            tmp_data = regress_result.loc[factor, [f't_{period}', f'coef_{period}']].copy()
            _reg_plot(tmp_data, axes[i, j])

    if path is not None:
        plt.savefig(path, bbox_inches='tight')
    else:
        plt.show()

def ic_plot(ic_result: pd.DataFrame, path: str = None) -> None:
    ''''''
    def _ic_plot(d, ax):
        d.index = pd.to_datetime(d.index)
        ax.set_title(f'ic: {factor} - {period}', fontsize=20)
        ax.bar(d.index, d[f'ic_{period}'], color='b', width=8, label=f'ic_{period}')
        ax.hlines(0, d.index[0], d.index[-1], color='k', linestyle='-.')
        ax.legend(loc='best')
        
    factors = ic_result.index.levels[0]
    periods = set(ic_result.columns.map(lambda x: x.split('_')[-1]))
    
    _, axes = plt.subplots(len(factors), len(periods), 
        figsize=(len(periods) * 17, len(factors) * 10))
    if len(factors) == 1 and len(periods) == 1:
        axes = np.array([[axes]])
    elif len(factors) == 1 and len(periods) > 1:
        axes = axes.reshape(1, -1)
    elif len(factors) > 1 and len(periods) == 1:
        axes = axes.reshape(-1, 1)

    for i, factor in enumerate(factors):
        for j, period in enumerate(periods):
            tmp_data = ic_result.loc[factor, [f'ic_{period}']].copy()
            _ic_plot(tmp_data, axes[i, j])

    if path is not None:
        plt.savefig(path, bbox_inches='tight')
    else:
        plt.show()

def layering_plot(layering_result: pd.DataFrame, path: str = None) -> None:
    ''''''
    def _layer_plot(d, ax):
        d = d.unstack(level=0)
        quantiles = d.columns.get_level_values(1).unique()
        ax.set_title(f'layering: {factor} - {period}', fontsize=20)
        for q in quantiles:
            bottom = d[(f'profit_{period}', q - 1)] if q > 1 else [0] * len(d.index)
            ax.bar(d.index, d[(f'profit_{period}', q)], bottom=bottom, width=8, label=q)
        ax.hlines(0, d.index[0], d.index[-1], color='k', linestyle='-.')
        ax.legend(loc='best')
        ax = ax.twinx()
        for q in quantiles:
            ax.plot(d.index, d[(f'cum_profit_{period}', q)], label=q)
        ax.legend(loc='best')
        
    factors = layering_result.index.levels[0]
    periods = set(layering_result.columns.map(lambda x: x.split('_')[-1]))
    
    _, axes = plt.subplots(len(factors), len(periods), 
        figsize=(len(periods) * 17, len(factors) * 10))
    if len(factors) == 1 and len(periods) == 1:
        axes = np.array([[axes]])
    elif len(factors) == 1 and len(periods) > 1:
        axes = axes.reshape(1, -1)
    elif len(factors) > 1 and len(periods) == 1:
        axes = axes.reshape(-1, 1)

    for i, factor in enumerate(factors):
        for j, period in enumerate(periods):
            tmp_data = layering_result.loc[factor, [f'profit_{period}', f'cum_profit_{period}']].copy()
            _layer_plot(tmp_data, axes[i, j])

    if path is not None:
        plt.savefig(path, bbox_inches='tight')
    else:
        plt.show()


if __name__ == '__main__':
    factors = ['return_1m', 'return_3m']
    dates = trade_date('2011-02-01', '2011-12-31', freq='monthly')
    forward_period = [1, 3]
    data = factor_datas_and_forward_returns(factors, dates, forward_period)
    reg_results = regression(data)
    ic_results = ic(data)
    layer_results = layering(data)
    regression_plot(reg_results, 'test_reg.png')
    ic_plot(ic_results, 'test_ic.png')
    layering_plot(layer_results, 'test_layer.png')
