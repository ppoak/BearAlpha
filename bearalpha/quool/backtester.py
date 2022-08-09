import sys
import pandas as pd
import numpy as np
import backtrader as bt
import matplotlib.pyplot as plt
from .base import *
from ..tools import *


class BackTesterError(FrameWorkError):
    pass


@pd.api.extensions.register_dataframe_accessor("relocator")
@pd.api.extensions.register_series_accessor("relocator")
class Relocator(Worker):

    def _valid(self, data: 'pd.DataFrame | pd.Series'):
        if self.ists(data) and self.isframe(data):
            return data.stack()
        elif self.ispanel(data) and self.isseries(data):
            return data.copy()
        elif self.ispanel(data) and self.isframe(data) and data.columns.size == 1:
            return data.iloc[:, 0].copy()
        else:
            raise BackTesterError('profit', 'Your weight data should either be in PN series or TS frame form')
    
    def profit(
        self, 
        ret: pd.Series, 
        portfolio: pd.Series = None,
    ):
        """calculate profit from weight and forward
        ---------------------------------------------

        ret: pd.Series, the return data in either PN series or TS frame form
        portfolio: pd.Series, the portfolio tag marked by a series, 
            only available when passing a PN
        """
        
        weight = self._valid(self.data)
        weight = weight.groupby(level=0).apply(lambda x: x / x.sum())
        ret = self._valid(ret)

        if portfolio is not None:
            portfolio = self._valid(portfolio)
                
        if portfolio is not None:
            grouper = [portfolio, pd.Grouper(level=0)]
        else:
            grouper = pd.Grouper(level=0) 
        
        return weight.groupby(grouper).apply(lambda x: 
            (ret.loc[x.index] * x).sum() / x.sum())
    
    def networth(
        self, 
        price: 'pd.Series | pd.DataFrame'
    ):
        """Calculate the networth curve using normal price data
        --------------------------------------------------------

        price: pd.Series or pd.DataFrame, the price data either in
            MultiIndex form or the TS Matrix form
        return: pd.Series, the networth curve
        """
        weight = self._valid(self.data)
        weight = weight.groupby(level=0).apply(lambda x: x / x.sum())
        price = self._valid(price)
            
        relocate_date = weight.index.levels[0]
        datetime_index = price.index.levels[0]
        lrd = relocate_date[0]
        lnet = (price.loc[d] * self.data.loc[lrd]).sum()
        lcnet = 1
        net = pd.Series(np.ones(datetime_index.size), index=datetime_index)
        for d in datetime_index[1:]:
            cnet = (price.loc[d] * self.data.loc[lrd]).sum() / lnet * lcnet
            lrd = relocate_date[relocate_date <= d][-1]
            if d == lrd:
                lcnet = cnet
                lnet = (price.loc[d] * self.data.loc[lrd]).sum()
            net.loc[d] = cnet
        return net
        
    def turnover(self, side: str = 'both'):
        """calculate turnover
        ---------------------

        side: str, choice between "buy", "short" or "both"
        """
        weight = self._valid(self.data)
        weight = weight.groupby(level=0).apply(lambda x: x / x.sum())
        weight = weight.reindex(pd.MultiIndex.from_product(
            [weight.index.levels[0], weight.index.levels[1]],
            names = ['date', 'asset'],
        ))
        delta = pd.concat([weight, weight.groupby(level=1).shift(1)], axis=0, 
            keys=['cur', 'pre']).unstack(level=0).fillna(0)
        delta = delta.iloc[:, 0] - delta.iloc[:, -1]
        if side == 'both':
            return delta.groupby(level=0).apply(lambda x: x.abs().sum())
        elif side == 'buy':
            return delta.groupby(level=0).apply(lambda x: x[x > 0].abs().sum())
        elif side == 'sell':
            return delta.groupby(level=0).apply(lambda x: x[x < 0].abs().sum())


@pd.api.extensions.register_dataframe_accessor("backtrader")
@pd.api.extensions.register_series_accessor("backtrader")
class BackTrader(Worker):
    """Backtester is a staff dedicated for run backtest on a dataset"""

    def _valid(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.isframe(data) and not 'close' in data.columns:
            raise BackTesterError('run', 'Your data should at least have a column named close')
        if self.type_ == Worker.CSSR or self.type_ == Worker.CSFR:
            raise BackTesterError('run', 'Cross section data cannot be used to run backtest')
        
        required_col = ['open', 'high', 'low']
        if self.isframe(data):
            # you should at least have a column named close
            for col in required_col:
                if not col in data.columns and col != 'volume':
                    data[col] = data['close']
            if not 'volume' in data.columns:
                data['volume'] = 0
        else:
            # just a series, all ohlc data will be the same, volume set to 0
            data = data.to_frame(name='close')
            for col in required_col:
                data[col] = col
            data['volume'] = 0

        return data

    def run(
        self, 
        strategy: bt.Strategy = None, 
        cash: float = 1000000,
        indicators: 'bt.Indicator | list' = None,
        analyzers: 'bt.Analyzer | list' = None,
        observers: 'bt.Observer | list' = None,
        coc: bool = False,
        image_path: str = None,
        data_path: str = None,
        show: bool = True,
    ):
        """Run a strategy using backtrader backend
        -----------------------------------------
        
        strategy: bt.Strategy
        cash: int, initial cash
        spt: int, stock per trade, defining the least stocks in on trade
        indicators: bt.Indicator or list, a indicator or a list of them
        analyzers: bt.Analyzer or list, an analyzer or a list of them
        observers: bt.Observer or list, a observer or a list of them
        coc: bool, to set whether cheat on close
        image_path: str, path to save backtest image
        data_path: str, path to save backtest data
        show: bool, whether to show the result
        """
        
        data = self.data.copy()
        data = self._valid(data)
        indicators = item2list(indicators)
        analyzers = [bt.analyzers.SharpeRatio, bt.analyzers.TimeDrawDown, bt.analyzers.TimeReturn, OrderTable]\
            if analyzers is None else item2list(analyzers)
        observers = [bt.observers.DrawDown]\
            if observers is None else item2list(observers)
                
        more = set(data.columns.to_list()) - set(['open', 'high', 'low', 'close', 'volume'])

        class _PandasData(bt.feeds.PandasData):
            lines = tuple(more)
            params = tuple(zip(more, [-1] * len(more)))
        
        cerebro = bt.Cerebro()
        cerebro.broker.setcash(cash)
        if coc:
            cerebro.broker.set_coc(True)
        
        # add data
        if self.type_ == Worker.PNFR or self.type_ == Worker.PNSR:
            datanames = data.index.levels[1].to_list()
        else:
            datanames = ['data']
        for dn in datanames:
            d = data.loc(axis=0)[:, dn].droplevel(1) if (self.type_ 
                == Worker.PNFR or self.type_ == Worker.PNSR) else data
            feed = _PandasData(dataname=d, fromdate=d.index.min(), todate=d.index.max())
            cerebro.adddata(feed, name=dn)
        
        if indicators is not None:
            for indicator in indicators:
                cerebro.addindicator(indicator)
        if strategy is not None:
            cerebro.addstrategy(strategy)
        for analyzer in analyzers:
            cerebro.addanalyzer(analyzer)
        for observer in observers:
            cerebro.addobserver(observer)
        
        result = cerebro.run()

        timereturn = pd.Series(result[0].analyzers.timereturn.rets)
        CONSOLE.print(dict(result[0].analyzers.sharperatio.rets))
        CONSOLE.print(dict(result[0].analyzers.timedrawdown.rets))
        figs = cerebro.plot(style='candel')
        if image_path is not None:
            fig = figs[0][0]
            fig.set_size_inches(18, 3 + 6 * len(datanames))
            fig.savefig(image_path, dpi=300)

        if show:
            if not timereturn.empty:
                timereturn.printer.display(title='time return')
                (timereturn + 1).cumprod().drawer.draw(kind='line')
                plt.show()
            if not result[0].analyzers.ordertable.rets.empty:
                result[0].analyzers.ordertable.rets.printer.display(title='order table')
        if data_path is not None:
            with pd.ExcelWriter(data_path) as writer:
                timereturn.to_excel(writer, sheet_name='TimeReturn')
                result[0].analyzers.ordertable.rets.to_excel(writer, sheet_name='OrderTable')

    def relocate(
        self,
        portfolio: 'pd.DataFrame | pd.Series' = None,
        spt: int = 100,
        ratio: float = 0.05,
        cash: float = 1000000,
        analyzers: 'bt.Analyzer | list' = None,
        observers: 'bt.Observer | list' = None,
        coc: bool = False,
        image_path: str = None,
        data_path: str = None,
        show: bool = True,
    ):
        """Test directly from dataframe position information
        -----------------------------------------
        
        portfolio: pd.DataFrame or pd.Series, position information
        spt: int, stock per trade, defining the least stocks in on trade
        ratio: float, retention ration for cash, incase of failure in order
        cash: int, initial cash
        indicators: bt.Indicator or list, a indicator or a list of them
        analyzers: bt.Analyzer or list, an analyzer or a list of them
        observers: bt.Observer or list, a observer or a list of them
        coc: bool, to set whether cheat on close
        image_path: str, path to save backtest image
        data_path: str, path to save backtest data
        show: bool, whether to show the result
        """
        data = self.data.copy()
        data = self._valid(data)
        data = data.apply(lambda x: x * spt if x.name != 'volume' and x.name != 'openinterest' 
            else x / spt)
        analyzers = [bt.analyzers.SharpeRatio, bt.analyzers.TimeDrawDown, bt.analyzers.TimeReturn, OrderTable]\
            if analyzers is None else item2list(analyzers)
        observers = [bt.observers.DrawDown]\
            if observers is None else item2list(observers)

        if self.isframe(portfolio) and self.ists(portfolio):
            portfolio = portfolio.stack()
        elif self.isseries(portfolio) and self.ispanel(portfolio):
            portfolio = portfolio.copy()
        portfolio.name = 'portfolio'
        
        data = pd.concat([data, portfolio], axis=1, join='outer')
        data['portfolio'] = data['portfolio'].groupby(level=1).ffill()
        data['portfolio'] = data['portfolio'].groupby(level=0).apply(lambda x: x / x.sum())

        class _RelocateStra(Strategy):

            def __init__(self) -> None:
                self.holdings = pd.Series(np.zeros(len(datanames)), index=datanames, name='holdings')

            def next(self):
                target = pd.Series(dict([(d._name, d.portfolio[0]) for d in self.datas]), name='target')
                dec = target[target < self.holdings]
                inc = target[target > self.holdings]
                for d in dec.index:
                    self.order_target_percent(data=d, target=target.loc[d] * (1 - ratio), name=d)
                for i in inc.index:
                    self.order_target_percent(data=i, target=target.loc[i] * (1 - ratio), name=i)
                self.holdings = target

        class _RelocateData(bt.feeds.PandasData):
            lines = ('portfolio', )
            params = (('portfolio', -1), )

        cerebro = bt.Cerebro()
        cerebro.broker.setcash(cash)
        if coc:
            cerebro.broker.set_coc(True)

        # add data
        if self.type_ == Worker.PNFR or self.type_ == Worker.PNSR:
            datanames = data.index.levels[1].to_list()
        else:
            datanames = ['data']
        for dn in datanames:
            d = data.loc(axis=0)[:, dn].droplevel(1) if (self.type_ 
                == Worker.PNFR or self.type_ == Worker.PNSR) else data
            feed = _RelocateData(dataname=d, fromdate=d.index.min(), todate=d.index.max())
            cerebro.adddata(feed, name=dn)
        
        cerebro.addstrategy(_RelocateStra)
        for analyzer in analyzers:
            cerebro.addanalyzer(analyzer)
        for observer in observers:
            cerebro.addobserver(observer)
        
        result = cerebro.run()

        timereturn = pd.Series(result[0].analyzers.timereturn.rets)
        CONSOLE.print(dict(result[0].analyzers.sharperatio.rets))
        CONSOLE.print(dict(result[0].analyzers.timedrawdown.rets))
        figs = cerebro.plot(style='candel')
        if image_path is not None:
            fig = figs[0][0]
            fig.set_size_inches(18, 3 + 6 * len(datanames))
            fig.savefig(image_path, dpi=300)

        if show:
            if not timereturn.empty:
                timereturn.printer.display(title='time return')
                (timereturn + 1).cumprod().drawer.draw(kind='line')
                plt.show()
            if not result[0].analyzers.ordertable.rets.empty:
                result[0].analyzers.ordertable.rets.printer.display(title='order table')
        if data_path is not None:
            with pd.ExcelWriter(data_path) as writer:
                timereturn.to_excel(writer, sheet_name='TimeReturn')
                result[0].analyzers.ordertable.rets.to_excel(writer, sheet_name='OrderTable')


@pd.api.extensions.register_dataframe_accessor("factester")
@pd.api.extensions.register_series_accessor("factester")
class Factester(Worker):

    def _valid(self, data: 'pd.DataFrame | pd.Series', name: str):
        if data is None:
            return None
        elif self.ists(data) and self.isframe(data):
            CONSOLE.print(f'[yellow][!][/yellow] {name} in wide form, transposing ... ')
            data = data.stack()
        elif self.ispanel(data) and self.isseries(data):
            data = data.copy()
        elif self.ispanel(data) and self.isframe(data) and data.columns.size == 1:
            data = data.iloc[:, 0]
        else:
            raise BackTesterError('Factester', 'Your data cannot be converted to a standarized factor data')

        data.name = name
        return data
    
    def _csanalyze(
        self,
        factor: pd.Series, 
        forward: pd.Series,
        grouper: pd.Series = None, 
        plot_period: 'int | str' = -1,
        scatter_ax: plt.Axes = None, 
        boxplot_ax: plt.Axes = None, 
        hist_ax: plt.Axes = None
    ):
        concated_data = pd.concat([factor, forward, grouper], axis=1, join='inner')
        datetime_index = concated_data.dropna().index.get_level_values(0).unique()
        if isinstance(plot_period, int):
            plot_period = datetime_index[plot_period]

        if boxplot_ax is not None and grouper is not None:
            concated_data.drawer.draw('box', datetime=plot_period, whis=(5, 95),
                by=grouper.name, ax=boxplot_ax, indicator=[factor.name, grouper.name])
        if scatter_ax is not None:
            concated_data.drawer.draw('scatter', datetime=plot_period,
                x=factor.name, y=forward.name, ax=scatter_ax, s=1)
        if hist_ax is not None:
            # if grouper is not None:
            #     for group in grouper.dropna().unique():
            #         group_cs = concated_data.loc[plot_period].loc[
            #             (concated_data[grouper.name] == group).loc[plot_period], factor.name]
            #         if group_cs.empty:
            #             continue
            #         group_cs.drawer.draw('hist', bins=100, ax=hist_ax, label=group, indicator=factor.name, alpha=0.7)
            # else:
            concated_data.loc[plot_period].drawer.draw('hist', ax=hist_ax, 
                indicator=factor.name, bins=100, alpha=0.7)
            # hist_ax.legend()

    def _branalyze(
        self,
        factor: pd.Series, 
        forward: pd.Series,
        marketcap: pd.Series,
        grouper: pd.Series, 
        data_writer: pd.ExcelWriter = None,
        barra_ax: plt.Axes = None,
        show: bool = True
    ) -> None:
        freq = forward.index.levels[0].freq.n - 1
        grouper_dummies = pd.get_dummies(grouper).iloc[:, 1:]
        barra_x = pd.concat([grouper_dummies, factor], axis=1)
        barra_y = forward
        weight = 1 / np.sqrt(marketcap)
        barra_result = barra_x.regressor.wls(barra_y, weight)
        barra_result = barra_result.groupby(level=0).apply(lambda x: 
            pd.DataFrame({"coef": x.iloc[0].params, "t": x.iloc[0].tvalues, "p": x.iloc[0].pvalues}))
        barra_sigtest = barra_result.loc[:, 'coef'].tester.sigtest().unstack()
        barra_sigtest['abs(t(coef)) > 2'] = barra_result.loc[:, 't'].groupby(level=1
            ).apply(lambda x: x[x.abs() >= 2].count() / x.count())

        if show:
            barra_result.round(4).printer.display(title='barra result', asset=factor.name)
            barra_sigtest.round(4).printer.display(title='barra sigtest')

        if data_writer is not None:
            barra_result.to_excel(data_writer, sheet_name=f'barra result {freq}D')
            barra_sigtest.to_excel(data_writer, sheet_name=f'barra sigtest {freq}D')

        if barra_ax is not None:
            barra_ax.set_title(f'barra test {freq}D')
            barra_result.drawer.draw('bar', ax=barra_ax, asset=factor.name, indicator='coef', width=5)
            barra_result.drawer.draw('line', color='#aa1111', 
                ax=barra_ax.twinx(), asset=factor.name, indicator='t')

    def _icanalyze(
        self,
        factor: pd.Series, 
        forward: pd.Series,
        grouper: pd.Series = None, 
        data_writer: pd.ExcelWriter = None,
        ic_ax: plt.Axes = None, 
        show: bool = True
    ) -> int:
        freq = forward.index.levels[0].freq.n - 1

        if grouper is not None:
            ic = factor.describer.ic(forward=forward, grouper=grouper)
            ic = ic.loc[ic.index.get_level_values(1) != 'nan'].iloc[:, 0]
            ic = pd.concat([factor.describer.ic(forward), ic.unstack()], axis=1)
        else:
            ic = factor.describer.ic(forward)

        evaluation = ic.sigtester.ttest(0)
        sigportion = ic[ic.abs() >= 0.03].count() / ic.count()
        if isinstance(evaluation, pd.Series):
            evaluation['abs(ic) > 0.03'] = sigportion.iloc[0]
        else:
            evaluation['abs(ic) > 0.03'] = sigportion
        
        if show:
            ic.round(4).printer.display(title=f'IC {freq}D')
            evaluation.round(4).printer.display(title=f'IC sigtest {freq}D')

        if data_writer is not None:
            ic.to_excel(data_writer, sheet_name=f'IC {freq}D')
            evaluation.to_excel(data_writer, sheet_name=f'IC sigtest {freq}D')
                
        if ic_ax is not None:
            ic.drawer.draw('bar', ax=ic_ax, width=3, indicator=factor.name)
            ic.rolling(12).mean().drawer.draw('line', ax=ic_ax, 
                title=f'IC test {freq}D', indicator=factor.name)
            ic_ax.hlines(y=0.03, xmin=ic_ax.get_xlim()[0], 
                xmax=ic_ax.get_xlim()[1], color='#aa3333', linestyle='--')
            ic_ax.hlines(y=-0.03, xmin=ic_ax.get_xlim()[0], 
                xmax=ic_ax.get_xlim()[1], color='#aa3333', linestyle='--')
        
        # Return the factor direction, for layering to construct long short portfolio
        return np.sign(ic.iloc[:, 0].mean())

    def _lranalyze(
        self,
        factor: pd.Series, 
        forward: pd.Series, 
        q: int = 5, 
        direction: int = 1, 
        commission_type: str = 'both', 
        commission: float = 0.001,
        benchmark: pd.Series = None, 
        data_writer: pd.ExcelWriter = None,
        layering_ax: plt.Axes = None,
        turnover_ax: plt.Axes = None, 
        show: bool = True
    ) -> None:
        freq = forward.index.levels[0].freq.n - 1
        # TODO: finish layering within group
        factor = direction * factor
        quantiles = factor.groupby(level=0).apply(pd.qcut, q=q, labels=False) + 1
        weight = pd.Series(np.ones_like(quantiles), index=quantiles.index)
        profit = weight.groupby(quantiles).apply(
            lambda x: x.relocator.profit(forward)).swaplevel().sort_index()
        turnover = weight.groupby(quantiles).apply(
            lambda x: x.relocator.turnover(side=commission_type)).swaplevel().sort_index()
        profit = profit - turnover * commission
        profit = profit.groupby(level=1).shift(1).fillna(0).unstack()
        profit['long_short'] = profit.iloc[:,-1] - profit.iloc[:,0]
        netvaluecurve = (profit + 1).cumprod()
        
        if benchmark is not None:
            benchmark_netvalue = benchmark / benchmark.iloc[0]
        else:
            benchmark_netvalue = netvaluecurve.iloc[:,0].copy()
            benchmark_netvalue.name = 'benchmark'
            benchmark_netvalue[:] = 1
        netvaluecurve = pd.concat([netvaluecurve, benchmark_netvalue], axis=1).dropna()
        benchmark_profit = netvaluecurve.iloc[:,-1].pct_change().fillna(0)
        profit = pd.concat([profit, benchmark_profit], axis=1).dropna()

        # TODO: Find something to present the risk free rate, and apply it here
        rf = pd.Series([0.04] * netvaluecurve.shape[0], index=netvaluecurve.index) * freq / 252

        premium = profit.apply(lambda x: x - rf)
        sharpe = premium.mean() / premium.std()
        if benchmark is None:
            sharpe['benchmark'] = np.nan
        sharpe.name = 'sharpe'

        drawdown = netvaluecurve - netvaluecurve.cummax()
        maxdrawdown = drawdown.min().abs()
        maxdrawdown.name = 'maxdrawdown'

        var95 = profit.quantile(.05)
        var95.name = 'var95'

        win_rate = profit.apply(lambda x: x > benchmark_profit).sum() / len(profit)
        win_rate.name = 'win_rate'

        date_size = pd.date_range(start=profit.index[0], end=profit.index[-1], freq=chinese_trading_days()).size
        annual_ret = (netvaluecurve.iloc[-1] / netvaluecurve.iloc[0] - 1) * 252 / date_size
        annual_ret.name = 'annual_ret'

        evaluation = pd.concat([sharpe, maxdrawdown, var95, win_rate, annual_ret], axis=1)
        
        if show:
            profit.round(4).printer.display(title=f'profit {freq}D')
            netvaluecurve.round(4).printer.display(title=f'cumulative profit {freq}D')
            evaluation.round(4).printer.display(title=f'evaluation {freq}D')
            turnover.round(2).printer.display(title=f'turnover {freq}D')

        if data_writer is not None:
            profit_data = pd.concat([profit.stack(), netvaluecurve.stack()], axis=1)
            profit_data.index.names = ['datetime', 'quantiles']
            profit_data.columns = ['profit', 'netvaluecurve']
            profit_data.unstack().to_excel(data_writer, sheet_name=f'layering {freq}D')

        if layering_ax is not None:
            layering_ax_twinx = layering_ax.twinx()
            profit.drawer.draw('bar', ax=layering_ax, width=2)
            netvaluecurve.drawer.draw('line', ax=layering_ax_twinx, title=f'layering test {freq}D')
            # layering_ax_twinx.hlines(y=1, xmin=layering_ax.get_xlim()[0], 
            #     xmax=layering_ax.get_xlim()[1], color='grey', linestyle='--')

        if turnover_ax is not None:
            turnover.unstack().drawer.draw('line', ax=turnover_ax, title=f'turnover {freq}D')

        if data_writer is not None:
            turnover.unstack().to_excel(data_writer, sheet_name=f'turnover {freq}D')

    def analyze(
        self,
        price: 'pd.Series | pd.DataFrame',
        marketcap: 'pd.Series | pd.DataFrame' = None,
        grouper: 'pd.Series | pd.DataFrame | dict' = None, 
        benchmark: pd.Series = None,
        periods: 'list | int' = [5, 10, 15], 
        q: int = 5, 
        commission: float = 0.001, 
        commission_type: str = 'both', 
        plot_period: 'int | str' = -1, 
        data_path: str = None, 
        image_path: str = None, 
        show: bool = True
    ):
        factor = self.data.copy()
        factor = self._valid(factor, 'factor')
        price = self._valid(price, 'price')
        grouper = self._valid(grouper, 'grouper') if grouper is not None else None
        marketcap = self._valid(marketcap, 'marketcap') if grouper is not None else None

        periods = item2list(periods)
        data_writer = pd.ExcelWriter(data_path) if data_path is not None else None
        fig, axes = plt.subplots(7, len(periods), figsize=(12 * len(periods), 7 * 8))
        if sys.platform == 'linux':
            plt.rcParams['font.sans-serif'] = ['DejaVu Serif']
        elif sys.platform == 'darwin':
            plt.rcParams['font.sans-serif'] = ['STSong']
            plt.rcParams['axes.unicode_minus'] = False
        elif sys.platform == 'win32':
            plt.rcParams['font.sans-serif'] = ['SimHei']
            plt.rcParams['axes.unicode_minus'] = False

        if data_writer is not None:
            factor.filer.to_excel(data_writer, sheet_name=f'factor_data')
        
        for i, period in enumerate(periods):
            forward_return = price.converter.price2ret(period=(-period - 1) * chinese_trading_days())
            # slice the common part of data
            CONSOLE.print(f'[green][PERIOD = {period}][/green] Filtering common part ... ')
            common_index = factor.index.intersection(forward_return.index)
            common_index = common_index.intersection(grouper.index) if grouper is not None else common_index
            common_index = common_index.intersection(marketcap.index) if marketcap is not None else common_index
            
            factor_data_period = factor.loc[common_index]
            forward_return_period = forward_return.loc[common_index]
            grouper_period = grouper.loc[common_index] if grouper is not None else None
            marketcap_period = marketcap.loc[common_index] if marketcap is not None else None
            
            self._csanalyze(
                factor=factor_data_period, 
                forward=forward_return_period, 
                grouper=grouper_period, 
                plot_period=plot_period, 
                boxplot_ax=axes[0, i], 
                scatter_ax=axes[1, i], 
                hist_ax=axes[2, i]
            )
                                
            CONSOLE.rule(f'[PERIOD = {period}] Barra Test')
            if grouper_period is not None and marketcap_period is not None:
                self._branalyze(
                    factor=factor_data_period, 
                    forward=forward_return_period, 
                    marketcap=marketcap_period, 
                    grouper=grouper_period,
                    data_writer=data_writer, 
                    barra_ax=axes[3, i], 
                    show=show
                )
            else:
                CONSOLE.print('[yellow][!][/yellow] You didn\'t provide group information,'
                    'so it is impossible to make barra test')
                        
            CONSOLE.rule(f'[PERIOD = {period}] IC Test')
            factor_direction = self._icanalyze(
                factor=factor_data_period, 
                forward=forward_return_period, 
                grouper=grouper_period, 
                data_writer=data_writer, 
                ic_ax=axes[4, i], 
                show=show
            )
                    
            CONSOLE.rule(f'[PERIOD = {period}] Layering Test')
            self._lranalyze(
                factor=factor_data_period,
                forward=forward_return_period, 
                direction=factor_direction,
                q=q, 
                commission=commission, 
                commission_type=commission_type,
                benchmark=benchmark, 
                data_writer=data_writer, 
                layering_ax=axes[5, i], 
                turnover_ax=axes[6, i], 
                show=show
            )

        if image_path is not None:
            plt.savefig(image_path, bbox_inches='tight', dpi=fig.dpi, pad_inches=0.0)
        if show:
            plt.show()
        if data_writer is not None:
            data_writer.close()
    