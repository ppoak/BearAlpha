#!python -m utils.treatment
import numpy as np


def median_correct(data, n=5):
    """中位数去极值的方法
    -------------------------

    传统的去极值方法因为涉及到标准差的计算，由于
    极值的存在导致标准差已经存在偏差，因此采用中位数去极值的方法，
    可以产生相对于中位数去极值方法更为稳健的结果

    Parameters:
    -----------

    data: np.array, 原始数据
    n: int, 距离中位数的偏差

    Return:
    ------------

    np.array, 中位数去极值后的结果
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


def deextreme(data, n=None, deextreme_type='median'):
    """去极值方法
    ----------------

    Parameters:
    ----------------

    data: np.array, 原始数据
    n: int, 
        当使用中位数去极值的方法时，n为数值点距离中位数的距离
        当使用固定比例去极值时，n必须为以下之一
            1. 列表形式的n表示保留在列表头尾的两个分位点之间的数据
            2. 浮点数形式的n表示去掉n比例的头尾数据
    deextreme_type: str, 必须为一下的一个
        'median': 中位数去极值
        'fix_odd': 固定比例去极值

    Return:
    ------------------

    np.array, 去极值后的数据
    """
    if deextreme_type == 'median':
        if not n:
            n = 5
        new_data = median_correct(data, n)
    elif deextreme_type == 'mean_std':
        # TODO: mean_std method
        pass
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
    """z分数标准化方法
    -------------------------

    zscore = (value - mean) / std

    Parameters:
    -----------

    data: np.array, 原始数据

    Return: 
    -----------
    np.array, 标准化后的数据
    """
    mean = np.mean(data[~np.isnan(data)])
    std = np.std(data[~np.isnan(data)])
    new_data = (data - mean) / std
    return new_data


def standard(data, standard_type='zscore'):
    """数据标准化的函数
    -----------------

    为了统一量纲同时方便回归，必须对原始因子数据进行标准化

    Parameters:
    -----------

    data: np.array, 原始数据
    standard_type: str, 回归方法必须是一下之一
        'zscore': z分数标准化方法
        'weight': 权重标准化方法
        'rank': 排序标准化法，更加稳健，能够保持因子数据的序关系

    Return:
    -----------
    np.array, 标准化后的因子数据
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
        raise ValueError(
            "type should be chosen in ['zscore', 'weight', 'rank']")
    return new_data


def missing_fill(data: np.array) -> np.array:
    """缺失值填充方法
    ---------------------

    Parameters:
    -----------

    data: np.array, 原始数据

    Return:
    -----------

    np.array, 将缺失值填充为0后的数据
    """
    new_data = data.copy()
    new_data[np.isnan(new_data)] = 0
    return new_data


if __name__ == '__main__':
    data = np.random.random(100000)
    data = np.append(data, np.nan)
    data = np.append(data, 10000)
    data2 = deextreme(data)
    data2 = standard(data2)
    data2 = missing_fill(data2)
    print(data2)

