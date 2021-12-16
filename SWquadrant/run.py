import pandas as pd
import numpy as np
import akshare as ak
import statsmodels.api as sm
import matplotlib.pyplot as plt
from matplotlib.font_manager import fontManager

def reg_font():
    font = fontManager.addfont("assets/font/xingshu.ttf")
    plt.rcParams['font.sans-serif'] = "FZXuJingLeiXingShuS"


def get_data(index1, index2):
    result1 = ak.stock_zh_index_daily(index1).set_index("date")
    result1.index = pd.to_datetime(result1.index)
    result2 = ak.stock_zh_index_daily(index2).set_index("date")
    result2.index = pd.to_datetime(result2.index)
    return result1, result2

def calc_rws(index1, index2, ma=10):
    index1_ret = index1.close / index1.close.shift(1) - 1
    index2_ret = index2.close / index2.close.shift(1) - 1
    index1_ret = index1_ret.replace(np.nan, 0) + 1
    index2_ret = index2_ret.replace(np.nan, 0) + 1
    index1_cumret = index1_ret.cumprod()
    index2_cumret = index2_ret.cumprod()
    rws = np.log(index1_cumret / index2_cumret)
    rws = rws.rolling(ma).mean()
    return rws

def reg(data):
    data = data.copy()
    x = np.arange(data.shape[0])
    x = sm.add_constant(x)
    y = data.values
    model = sm.OLS(y, x)
    result = model.fit()
    # return result.params[0], result.params[1]
    return result.params[1]

def ret(data):
    x2 = len(data)
    x1 = 0
    y2 = data.iloc[-1]
    y1 = data.iloc[0]
    inter = (x2 * y1 - x1 * y2) / (x2 - x1) 
    coef = (y2 - y1) / (x2 - x1)
    # return inter, coef
    return coef

def derivative(data, period=20, method="reg"):
    method_dict = {
        "reg": reg,
        "ret": ret
    }
    data = data.rolling(period).apply(method_dict[method])
    return data

def draw_derivative(data, derivative, ax):
    ax.plot(data)
    ax = ax.twinx()
    ax.plot(derivative)
    ax.hlines(0, xmin=derivative.index[0], xmax=derivative.index[-1])
    return ax

def draw_quadrant(x, y, start="2016-03-01", end="2016-04-01"):
    x = x.loc[start:end]
    y = y.loc[start:end]
    _, ax = plt.subplots(1, 1, figsize=(12, 8), dpi=300)
    ax.set_title("申万四象限风格轮动")
    common_index = x.index.intersection(y.index)
    ax.text(x=x.iloc[0], y=y.iloc[0], s=f'起始点:{x.index[0].strftime(r"%Y-%m-%d")}', color="skyblue")
    
    for i, date in enumerate(common_index[1:]):
        if i % 3 == 1:
            ax.text(x=x.loc[date], y=y.loc[date], s=date.strftime(r"%Y-%m-%d"), color="grey")
        last_date = common_index[i - 1]
        ax.plot(x.loc[[last_date, date]], y.loc[[last_date, date]], marker='o')

    ax.text(x=x.iloc[-1], y=y.iloc[-1], s=f'终止点:{x.index[-1].strftime(r"%Y-%m-%d")}', color="skyblue")

    xp = np.linspace(0, ax.get_xlim()[1], 10)
    xn = np.linspace(ax.get_xlim()[0], 0, 10)
    yp = np.ones_like(xp) * ax.get_ylim()[1]
    yn = np.ones_like(xn) * ax.get_ylim()[0]
    zeroy = np.zeros_like(yp)
    ax.fill_between(xp, yp, zeroy, color='red')
    ax.fill_between(xn, zeroy, yp, color='orange')
    ax.fill_between(xn, yn, zeroy, color='blue')
    ax.fill_between(xp, yn, zeroy, color='green')

    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.xaxis.set_ticks_position("bottom")
    ax.yaxis.set_ticks_position("left")
    ax.spines["bottom"].set_position(("data", 0))
    ax.spines["left"].set_position(("data", 0))

    plt.savefig("SWquadrant/quadrant.png")

if __name__ == "__main__":
    reg_font()
    index1 = "sh000043"
    index2 = "sh000045"
    big, small = get_data(index1, index2)
    rws = calc_rws(big, small)
    drws = derivative(rws)
    ddrws = derivative(drws)
    draw_quadrant(ddrws, drws)