import numpy as np


def median_correct(data, n=5):
    """deextrme using median
    -------------------------

    data: np.array, raw data
    n: int, n deviations from median
    return: np.array, result
    """
    md = np.median(data[~np.isnan(data)])
    MAD = np.median(np.abs(data[~np.isnan(data)] - md))
    up = md + n * MAD
    down = md - n * MAD
    new_data = data.copy()
    new_data[new_data > up] = up
    new_data[new_data < down] = down
    return new_data

def fix_odd_drop(data, n):
    drop_proportion = n / 2
    new_data = data.copy()
    new_data = new_data[(data >= data.quantile(drop_proportion)) & (
        data <= data.quantile(1 - drop_proportion))].droplevel('date')
    return new_data

def fix_odd_percentile(data, n):
    data_bot = n[0]
    data_top = n[1]
    new_data = data.copy()
    new_data = new_data[(data >= data.quantile(data_bot)) & (
        data <= data.quantile(data_top))].droplevel('date')
    return new_data

def mean_std(data, n):
    mean = np.mean(data[~np.isnan(data)])
    std = np.std(data[~np.isnan(data)])
    new_data = data.copy()
    new_data[new_data > mean + n * std] = mean + n * std
    new_data[new_data < mean - n * std] = mean - n * std
    return new_data

def deextreme(data, n=None, deextreme_type='mean_std'):
    """deextreme ways
    ----------------

    data: np.array, raw data
    n: int, 
        when using median, n is the number of deviations from median
        when using fixed odd percentile, n must be one of those:
            1. the remaining percentiles for head and tail in a list form
            2. the n percentiles for head and tail in a float form
        when using mean std, ni is the number of deviations from mean
    deextreme_type: str, must be one of these:
        'median': median way
        'fix_odd': fixed odd way
        'mean_std': mean_std way
    return: np.array, result
    """
    if deextreme_type == 'median':
        if not n:
            n = 5
        new_data = median_correct(data, n)
    elif deextreme_type == 'mean_std':
        new_data = mean_std(data, n)
    elif deextreme_type == 'fix_odd':
        if isinstance(n, float):
            new_data = fix_odd_drop(data, n)
        elif isinstance(n, list):
            assert len(n) == 2
            new_data = fix_odd_percentile(data, n)
    else:
        raise ValueError(
            "type should be chosen in ['median', 'mean_std', 'fix_odd']")
    return new_data

def zscore(data):
    """z-score standardization
    -------------------------

    data: np.array, raw data
    return: np.array, result
    """
    mean = np.mean(data[~np.isnan(data)])
    std = np.std(data[~np.isnan(data)])
    new_data = (data - mean) / std
    return new_data

def standard(data, standard_type='zscore'):
    """standardization ways
    -----------------

    data: np.array, raw data
    standard_type: str, standardization type must be one of these:
        'zscore': z-score standardization
        'weight': weight standardization
        'rank': rank standardization
    return: np.array, 标准化后的因子数据
    """
    if standard_type == 'zscore':
        new_data = zscore(data)
    elif standard_type == 'weight':
        # TODO: 市值加权标准化待完成
        pass
    elif standard_type == 'rank':
        # TODO: 排序标准化待完成
        pass
    else:
        raise ValueError("type should be chosen in ['zscore', 'weight', 'rank']")
    return new_data

def drop_na(data):
    new_data = data.copy()
    new_data = new_data[~np.isnan(data)]
    return new_data

def zero(data):
    new_data = data.copy()
    new_data[np.isnan(new_data)] = 0
    return new_data

def missing_fill(data: np.array, method: str = 'zero') -> np.array:
    """missing fill ways
    ---------------------

    data: np.array, raw data
    method: choices between ['drop', 'zero']
    return: np.array, missing data filled with 0
    """
    if method == 'drop':
        new_data = drop_na(data)
    elif method == 'zero':
        new_data = zero(data)
    return new_data


if __name__ == '__main__':
    data = np.random.random(100000)
    data = np.append(data, np.nan)
    data = np.append(data, 10000)
    data2 = deextreme(data)
    data2 = standard(data2)
    data2 = missing_fill(data2)
    print(data2)

