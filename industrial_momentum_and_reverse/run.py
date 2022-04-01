import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import font_manager
from typing import Union
from functools import lru_cache


font_manager.fontManager.addfont('行业动量与反转效应/font/STSong.ttf')
plt.rcParams['font.family'] = ['STSong']


def process_data(raw_data_path, target_data_dir) -> None:
    sheet_data = pd.read_excel(raw_data_path, sheet_name=None)
    for sheetname, data in sheet_data.items():
        data.to_csv(f'{target_data_dir}/{sheetname}.csv', index=False)

def logret2algret(logret: Union[pd.Series, pd.DataFrame, np.ndarray]) -> Union[pd.Series, pd.DataFrame, np.ndarray]:
    return np.exp(logret) - 1

def algret2logret(algret: Union[pd.Series, pd.DataFrame, np.ndarray]) -> Union[pd.Series, pd.DataFrame, np.ndarray]:
    return np.log(algret + 1)


class ReturnDecomposor():
    def __init__(self, industry):
        self.industry = industry
        self.data = pd.read_csv(f'行业动量与反转效应/data/processed/{industry}.csv',
            index_col=0, parse_dates=[0])

    def calc_fwd_ret(self):
        '''计算未来收益率'''
        close_price = self.data['收盘价(元)'].resample('m').last().dropna()
        ret = np.log(close_price.shift(-1) / close_price)
        ret.name = f'{self.industry}未来收益率'
        return ret.dropna().astype('float32')

    def calc_bwd_ret(self):
        '''计算原始收益率'''
        close_price = self.data['收盘价(元)'].resample('d').last().dropna()
        ret = np.log(close_price / close_price.shift(1))
        ret.name = f'{self.industry}原始收益率'
        return ret.dropna().astype('float32')
        
    def calc_overnight_ret(self):
        '''计算隔夜收益率'''
        close_price = self.data['收盘价(元)'].resample('d').last().dropna()
        open_price = self.data['开盘价(元)'].resample('d').first().dropna()
        overnight_ret = np.log(open_price / close_price.shift(1))
        overnight_ret.name = f'{self.industry}隔夜收益率'
        return overnight_ret.dropna().astype('float32')
    
    @lru_cache(maxsize=None)
    def __calc_intraday_ret(self):
        '''计算日内收益率'''
        intraday_ret = self.data['收盘价(元)'].resample('d').apply(lambda x: 
            np.log(x.values / x.shift(1).values))
        intraday_ret = intraday_ret.loc[intraday_ret.str.len() >= 1]
        intraday_ret.name = '日内收益率'
        return intraday_ret

    def __calc_gentle_extreme_ret(self, data: pd.Series, kind: str, diviation: float):
        '''计算温和极端收益率'''
        data = data.explode().dropna()
        md = np.median(data)
        mad = np.median(np.abs(data - md))
        mad_e = 1.4826 * mad
        if kind == 'gentle':
            ret = data.loc[np.abs(data - md) < diviation * mad_e]
        else:
            ret = data.loc[np.abs(data - md) >= diviation * mad_e]
        return ret.sum()

    def calc_gentle_ret(self, window_size: int = 1, diviation: float = 1.96):
        '''计算温和收益率'''
        intraday_ret = self.__calc_intraday_ret()
        gentle_ret = pd.Series(dtype=object)
        for i in range(window_size - 1, len(intraday_ret), window_size):
            gentle_ret.loc[intraday_ret.index[i]] = self.__calc_gentle_extreme_ret(
                intraday_ret.iloc[i - window_size + 1:i + 1], kind='gentle', diviation=diviation)
        gentle_ret.name = f'{self.industry}{window_size}温和收益率'
        return gentle_ret

    def calc_extreme_ret(self, window_size: int = 1, diviation: float = 1.96):
        '''计算极端收益率'''
        intraday_ret = self.__calc_intraday_ret()
        extreme_ret = pd.Series(dtype=object)
        for i in range(window_size - 1, len(intraday_ret), window_size):
            extreme_ret.loc[intraday_ret.index[i]] = self.__calc_gentle_extreme_ret(
                intraday_ret.iloc[i - window_size + 1:i + 1], kind='extreme', diviation=diviation)
        extreme_ret.name = f'{self.industry}{window_size}极端收益率'
        return extreme_ret
    
    @lru_cache(maxsize=None, typed=False)
    def get_ret(self, kind: str, window_size: int = 1, deviation: float = 1.96):
        '''获取收益率'''
        if kind == 'overnight':
            return self.calc_overnight_ret()
        elif kind == 'gentle':
            return self.calc_gentle_ret(window_size, deviation)
        elif kind == 'extreme':
            return self.calc_extreme_ret(window_size, deviation)
        elif kind == 'forward':
            return self.calc_fwd_ret()
        elif kind == 'backward' or kind == 'raw':
            return self.calc_bwd_ret()
        else:
            raise ValueError(f'{kind}收益不合法, 请使用overnight, gentle, extreme, forward, backward或raw')


class Analyzer():
    def __init__(self, industry_list):
        self.jk_range = [1, 3, 6, 9, 12]
        self.industry_list = industry_list
        self.dataset = {}
        for industry in industry_list:
            self.dataset[industry] = pd.read_csv(
                f'行业动量与反转效应/data/processed/{industry}.csv', 
                index_col=0, parse_dates=[0])
        self.decomposers = dict(zip(industry_list,
            [ReturnDecomposor(industry) for industry in industry_list]))

    @lru_cache(maxsize=None, typed=False)
    def get_ret(self, kind: str, window_size: int = 1, deviation: float = 1.96) -> pd.DataFrame:
        '''获取收益率'''
        ret = []
        for ind in self.industry_list:
            tmp = self.decomposers[ind].get_ret(kind, window_size, deviation)
            tmp.name = ind
            ret.append(tmp)
        return pd.concat(ret, axis=1).dropna(how='all')

    def get_retm(self, kind: str, window_size: int = 1, deviation: float = 1.96):
        '''获取月度收益率'''
        ret = self.get_ret(kind, window_size, deviation)
        ret = ret.resample('m').sum()
        return ret

    def descriptive_plot(self, window_size: Union[int, list], deviation: float):
        '''描述性统计绘图
        -----------------

        window_size: int or list, 可以提供一个列表或一个整数的窗口大小，作为子图返回在图片中
        deviation: int, 标准差的倍数，用于计算温和极端收益率的标准
        '''
        if isinstance(window_size, int):
            window_size = [window_size]
        
        monthly_ret = self.get_retm('backward')
        overnight_ret = self.get_ret('overnight')
        gentle_ret: list[pd.DataFrame] = []
        extreme_ret: list[pd.DataFrame] = []
        for ws in window_size:
            gentle_ret.append(self.get_ret('gentle', window_size=ws, deviation=deviation))
            extreme_ret.append(self.get_ret('extreme', window_size=ws, deviation=deviation))

        _, axes = plt.subplots(nrows=len(window_size), ncols=2, figsize=(30, 8 * len(window_size)))
        axes = axes.reshape((-1, 2))

        for i, ws in enumerate(window_size):
            gentle_ret_val = gentle_ret[i].values.reshape(-1)
            extreme_ret_val = extreme_ret[i].values.reshape(-1)
            pos_ex_ret = extreme_ret_val[extreme_ret_val > 0]
            neg_ex_ret = extreme_ret_val[extreme_ret_val <= 0]
            axes[i, 0].set_title(f'日内收益分布图, 窗口大小为{ws}')
            axes[i, 0].hist(gentle_ret_val, bins=200, label='温和收益率', alpha=0.6)
            axes[i, 0].hist(pos_ex_ret, bins=200, label='正向极端收益率', alpha=0.6)
            axes[i, 0].hist(neg_ex_ret, bins=200, label='负向极端收益率', alpha=0.6)
            axes[i, 0].legend()

        for i, ws in enumerate(window_size):
            monthly_ret_val = monthly_ret.values.reshape(-1)
            overnight_ret_val = overnight_ret.resample('1m').sum().values.reshape(-1)
            gentle_ret_val = gentle_ret[i].resample('1m').sum().values.reshape(-1)
            extreme_ret_val = extreme_ret[i].resample('1m').sum().values.reshape(-1)
            axes[i, 1].set_title(f'月度收益分布图')
            axes[i, 1].hist(monthly_ret_val, bins=200, label='月度总收益率', alpha=0.6)
            axes[i, 1].hist(overnight_ret_val, bins=200, label='月度隔夜收益率', alpha=0.6)
            axes[i, 1].hist(gentle_ret_val, bins=200, label='月度温和收益率', alpha=0.6)
            axes[i, 1].hist(extreme_ret_val, bins=200, label='月度极端收益率', alpha=0.6)
            axes[i, 1].legend()

        plt.savefig(f'行业动量与反转效应/image/descriptive.png')        
    
    def __jk_ret(self, kind: str, K: int, J: int, window_size: int, deviation: float):
        '''获取JK投资组合的收益率'''
        # 获取JK投资组合
        ret = self.get_retm(kind, window_size, deviation)
        lsn = 5
        lsport = []
        lport = []
        sport = []
        awport = []
        # lsport = pd.DataFrame(index=ret.index, columns=ret.columns)
        # lport = pd.DataFrame(index=ret.index, columns=ret.columns)
        # sport = pd.DataFrame(index=ret.index, columns=ret.columns)
        # awport = pd.DataFrame(index=ret.index, columns=ret.columns)
        for k in range(K):
            argsorted = np.argsort(ret.iloc[k:].rolling(J).sum().dropna(how='all'))
            lsp = pd.DataFrame(index=argsorted.index, columns=ret.columns)
            lp = pd.DataFrame(index=argsorted.index, columns=ret.columns)
            sp = pd.DataFrame(index=argsorted.index, columns=ret.columns)
            ap = pd.DataFrame(index=argsorted.index, columns=ret.columns)
            for idx in range(0, argsorted.index.shape[0], K):
                lsp.iloc[idx, argsorted.iloc[idx, :lsn]] = -1 / (lsn * K)
                lsp.iloc[idx, argsorted.iloc[idx, -lsn:]] = 1 / (lsn * K)
                lp.iloc[idx, argsorted.iloc[idx, -lsn:]] = 1 / (lsn * K)
                sp.iloc[idx, argsorted.iloc[idx, :lsn]] = -1 / (lsn * K)
                ap.iloc[idx, :] = 1 / len(argsorted.columns)
            
            lsport.append(lsp.shift(K))
            lport.append(lp.shift(K))
            sport.append(sp.shift(K))
            awport.append(ap.shift(K))
            
        # lsport = lsport.shift(K)
        # lport = lport.shift(K)
        # sport = sport.shift(K)
        # awport = awport.shift(K)
        raw_ret = self.get_retm('raw', window_size, deviation).rolling(K).sum()
        raw_ret = logret2algret(raw_ret)
        rets = []
        for port in [lsport, lport, sport, awport]:
            rets.append(pd.concat([(port[i] * raw_ret).dropna(how='all').sum(axis=1) 
                for i in range(K)], axis=1))
            # rets.append((port[0] * self.forward[0]).dropna(how='all').sum(axis=1).to_frame())

        ret_mean = [ret.mean(axis=1) for ret in rets[:-1]]
        ret_demean = [(rets[i] - rets[-1]).mean(axis=1) for i in range(len(rets) - 1)]
        t_mean = [rm.mean(axis=0) / rm.std(axis=0) * np.sqrt(len(rm)) for rm in ret_mean]
        t_demean = [rdm.mean(axis=0) / rdm.std(axis=0) * np.sqrt(len(rdm)) for rdm in ret_demean]

        self.retm = dict(zip(('lsport', 'lport', 'sport'), ret_mean))
        self.retdm = dict(zip(('lsport', 'lport', 'sport'), ret_demean))
        self.tm = dict(zip(('lsport', 'lport', 'sport'), t_mean))
        self.tdm = dict(zip(('lsport', 'lport', 'sport'), t_demean))
    
    def jk_ret_mat(self, kind: str, window_size: int = 1, deviation: float = 1.96,
                   port_type: str = 'lsport', demeaned: bool = False):
        '''获取JK投资组合的收益率矩阵'''
        jk_range = self.jk_range
        ret_mat = pd.DataFrame(index=jk_range, columns=jk_range)
        t_mat = pd.DataFrame(index=jk_range, columns=jk_range)

        for j in jk_range:
            for k in jk_range:
                self.__jk_ret(kind, K=k, J=j, window_size=window_size, deviation=deviation)
                if demeaned:
                    ret_mat.loc[j, k] = self.retdm[port_type].mean(axis=0)
                    t_mat.loc[j, k] = self.tdm[port_type]
                else:
                    ret_mat.loc[j, k] = self.retm[port_type].mean(axis=0)
                    t_mat.loc[j, k] = self.tm[port_type]
                
        return ret_mat, t_mat
        
    def jk_ret_plot(self, kind: str, window_size: int = 1, deviation: float = 1.96,
                    port_type: Union[str, list] = ['lsport', 'lport', 'sport'],
                    demeaned: bool = False):
        '''获取JK投资组合的收益率图'''
        if isinstance(port_type, str):
            port_type = [port_type]
        jk_range = self.jk_range

        _, axes = plt.subplots(len(jk_range), len(jk_range), figsize=(12 * len(jk_range), 8 * len(jk_range)))
        for m, j in enumerate(jk_range):
            for n, k in enumerate(jk_range):
                ax = axes[m, n]
                axt = ax.twinx()
                self.__jk_ret(kind, K=k, J=j, window_size=window_size, deviation=deviation)
                for pt in port_type:
                    if demeaned:
                        cum_ret = (self.retdm[pt] + 1).cumprod()
                        ax.plot(cum_ret.index, cum_ret.values, label=pt)
                        axt.bar(self.retdm[pt].index, self.retdm[pt].values, label=pt)
                        axt.hlines(self.retdm[pt].mean(), self.retdm[pt].index.min(), self.retdm[pt].index.max(), linewidth=0.5, linestyle='dashed')
                    else:
                        cum_ret = (self.retm[pt] + 1).cumprod()
                        ax.plot(cum_ret.index, cum_ret.values, label=pt)
                        axt.bar(self.retm[pt].index, self.retm[pt].values, label=pt)
                        axt.hlines(self.retm[pt].mean(), self.retm[pt].index.min(), self.retm[pt].index.max(), linewidth=0.5, linestyle='dashed')
                axt.hlines(0, self.retm[pt].index.min(), self.retm[pt].index.max(), color='black', linewidth=1)

                ax.legend()
                ax.set_title(f'kind={kind}, J={j}, K={k}, demeaned={demeaned}')

        if demeaned:
            plt.savefig(f'行业动量与反转效应/image/jk_ret_{kind}_dm.png')
        else:
            plt.savefig(f'行业动量与反转效应/image/jk_ret_{kind}_m.png')

if __name__ == "__main__":
    # import time
    # print(f"{'='*20} 读取原始数据并进行格式转换 {'='*20}")
    # start = time.time()
    # process_data('行业动量与反转效应/data/中信行业.xlsx',
        # '行业动量与反转效应/data/processed')
    # end = time.time()
    # print(f'{"="*20} 原始数据读取完毕，耗时{end - start}秒 {"="*20}')

    industry_list = [
        "CI005001.WI", "CI005002.WI", "CI005003.WI", "CI005004.WI",
        "CI005005.WI", "CI005006.WI", "CI005007.WI", "CI005008.WI",
        "CI005009.WI", "CI005010.WI", "CI005011.WI", "CI005012.WI",
        "CI005013.WI", "CI005014.WI", "CI005015.WI", "CI005016.WI",
        "CI005017.WI", "CI005018.WI", "CI005019.WI", "CI005020.WI",
        "CI005021.WI", "CI005023.WI", "CI005022.WI", "CI005024.WI",
        "CI005025.WI", "CI005026.WI", "CI005027.WI", "CI005028.WI",
        "CI005029.WI"
        ]
    analyzer = Analyzer(industry_list)

    # analyzer.descriptive_plot(window_size=[1, 3, 5, 10, 15, 20], deviation=1.96)

    for kind in ['raw', 'gentle', 'extreme', 'overnight']:
        print(f"{'-' * 10} 计算{kind}收益的JK组合收益矩阵中... {'-' * 10}")
        analyzer.jk_ret_plot(kind)
        ret_mat, t_mat = analyzer.jk_ret_mat(kind, port_type='lsport')
        ret_mat.to_excel(f'行业动量与反转效应/table/{kind}_ret_mat.xlsx')
        t_mat.to_excel(f'行业动量与反转效应/table/{kind}_t_mat.xlsx')
        print(ret_mat)
        print(t_mat)
    
    # for kind in ['raw', 'gentle', 'extreme', 'overnight']:
    #     print(f'{"-" * 10} 绘制{kind}JK收益率图中... {"-" * 10}')
    #     for j in [1, 6, 12]:
    #         for k in [1, 6, 12]:
    #             analyzer.jk_ret_plot(kind, k, j, port_type=['lsport', 'lport', 'sport'])
    #             analyzer.jk_ret_plot(kind, k, j, port_type=['lsport', 'lport', 'sport'], demeaned=True)
