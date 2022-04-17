import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Union
from .prepare import get_factor_columns


def price_plot(price: pd.Series, data: pd.DataFrame, assets: Union[str, list], save_path: str = None) -> None:
    '''Plot price and factor

    price: pd.DataFrame, price dataframe, the index must be multiindex,
        conform with the standard factor index, name must be `price`
    data: pd.DataFrame, standard factor data
    asset: str or list, asset name or list of asset name to plot
    '''
    if isinstance(assets, str):
        assets = [assets]
    
    data = pd.merge(price, data, left_index=True, right_index=True)
    data_to_plot = data.loc[(slice(None), assets), :]
    factors = get_factor_columns(data_to_plot)
    _, axes = plt.subplots(len(factors), 1, figsize=(12, 8 * len(factors)))
    if len(factors) == 1:
        axes = np.array([axes])
    
    for i, factor in enumerate(factors):
        axes[i].plot(data_to_plot.loc[(slice(None), assets), factor], label=factor)
        axes[i].plot(data_to_plot.loc[(slice(None), assets), 'price'], label='price')
        axes[i].set_title(f'{factor} and corresponding price', fontsize=20)
    
    if save_path:
        plt.savefig(save_path, bbox_inches='tight')
    else:
        plt.show()

