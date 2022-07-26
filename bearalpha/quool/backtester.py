import datetime
import pandas as pd
import numpy as np
import backtrader as bt
import matplotlib.pyplot as plt
from ..core import *
from ..tools import *

class BackTesterError(FrameWorkError):
    pass


@pd.api.extensions.register_dataframe_accessor("relocator")
@pd.api.extensions.register_series_accessor("relocator")
class Relocator(Worker):

    def __init__(self, data: 'pd.DataFrame | pd.Series'):
        super().__init__(data)
        weight = self.make_available(self.data)
        if isinstance(weight, bool):
            raise BackTesterError('profit', 'Your weight data should either be in PN series or TS frame form')
        self.weight = weight.groupby(level=0).apply(lambda x: x / x.sum())


    def make_available(self, data: 'pd.DataFrame | pd.Series'):
        if self.ists(data) and self.isframe(data):
            return data.stack()
        elif self.ispanel(data) and self.isseries(data):
            return data
        elif self.ispanel(data) and self.isframe(data) and data.columns.size == 1:
            return data.iloc[:, 0]
        else:
            return False
    
    def profit(
        self, 
        ret: pd.Series, 
        portfolio: pd.Series = None,
    ):
        '''calculate profit from weight and forward
        ---------------------------------------------

        ret: pd.Series, the return data in either PN series or TS frame form
        portfolio: pd.Series, the portfolio tag marked by a series, 
            only available when passing a PN
        '''
        
        weight = self.weight.copy()
        
        ret = self.make_available(ret)
        if isinstance(ret, bool):
            raise BackTesterError('profit', 'Your return data should either be in PN series or TS frame form')
        
        if portfolio is not None:
            portfolio = self.make_available(portfolio)
            if portfolio == False:
                raise BackTesterError('profit', 'Your portofolio data should either be in PN series or TS frame form')
                
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
        weight = self.weight.copy()
        price = self.make_available(price)
        if isinstance(price, bool):
            raise BackTesterError('networth', 'Price data should be in PN series or TS frame form')
            
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
        '''calculate turnover
        ---------------------

        side: str, choice between "buy", "short" or "both"
        '''
        weight = self.weight.copy()
        weight = weight.reindex(pd.MultiIndex.from_product(
            [weight.index.levels[0], weight.index.levels[1]],
            names = ['date', 'asset'],
        ))
        delta = pd.concat([weight, weight.groupby(level=1).shift(1)], axis=1, join='outer').fillna(0)
        delta = delta.iloc[:, 0] - delta.iloc[:, -1]
        if side == 'both':
            return delta.groupby(level=0).apply(lambda x: x.abs().sum())
        elif side == 'buy':
            return delta.groupby(level=0).apply(lambda x: x[x > 0].abs().sum())
        elif side == 'sell':
            return delta.groupby(level=0).apply(lambda x: x[x < 0].abs().sum())


class Strategy(bt.Strategy):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        '''Logging function'''
        datetime = datetime or self.data.datetime.date(0)
        datetime = time2str(datetime)
        if hint == "INFO":
            color = "color"
        elif hint == "WARN":
            color = "yellow"
        elif hint == "ERROR":
            color = "red"
        else:
            color = "blue"
        CONSOLE.print(f'[{color}][{hint}][/{color}] {datetime}: {text}')

    def notify_order(self, order: bt.Order):
        '''order notification'''
        # order possible status:
        # 'Created'、'Submitted'、'Accepted'、'Partial'、'Completed'、
        # 'Canceled'、'Expired'、'Margin'、'Rejected'
        # broker submitted or accepted order do nothing
        if order.status in [order.Submitted, order.Accepted, order.Created]:
            return

        # broker completed order, just hint
        elif order.status in [order.Completed]:
            self.log(f'Trade <{order.executed.size}> <{order.info.get("name", "data")}> at <{order.executed.price:.2f}>')
            # record current bar number
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            self.log('Order canceled, margin, rejected or expired', hint='WARN')

        # except the submitted, accepted, and created status,
        # other order status should reset order variable
        self.order = None

    def notify_trade(self, trade):
        '''trade notification'''
        if not trade.isclosed:
            # trade not closed, skip
            return
        # else, log it
        self.log(f'Gross Profit: {trade.pnl:.2f}, Net Profit {trade.pnlcomm:.2f}')


class Indicator(bt.Indicator):
    
    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        '''Logging function'''
        datetime = datetime or self.data.datetime.date(0)
        datetime = time2str(datetime)
        if hint == "INFO":
            color = "color"
        elif hint == "WARN":
            color = "yellow"
        elif hint == "ERROR":
            color = "red"
        else:
            color = "blue"
        CONSOLE.print(f'[{color}][{hint}][/{color}] {datetime}: {text}')
    

class Analyzer(bt.Analyzer):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        '''Logging function'''
        datetime = datetime or self.data.datetime.date(0)
        datetime = time2str(datetime)
        if hint == "INFO":
            color = "color"
        elif hint == "WARN":
            color = "yellow"
        elif hint == "ERROR":
            color = "red"
        else:
            color = "blue"
        CONSOLE.print(f'[{color}][{hint}][/{color}] {datetime}: {text}')


class Observer(bt.Observer):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        '''Logging function'''
        datetime = datetime or self.data.datetime.date(0)
        datetime = time2str(datetime)
        if hint == "INFO":
            color = "color"
        elif hint == "WARN":
            color = "yellow"
        elif hint == "ERROR":
            color = "red"
        else:
            color = "blue"
        CONSOLE.print(f'[{color}][{hint}][/{color}] {datetime}: {text}')


class OrderTable(Analyzer):

    def __init__(self):
        self.orders = pd.DataFrame(columns=['asset', 'size', 'price', 'direction'])
        self.orders.index.name = 'datetime'

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.orders.loc[self.data.datetime.date(0)] = [
                    order.info.get('name', 'data'), order.executed.size, 
                    order.executed.price, 'BUY']
            elif order.issell():
                self.orders.loc[self.data.datetime.date(0)] = [
                    order.info.get('name', 'data'), order.executed.size, 
                    order.executed.price, 'SELL']
        
    def get_analysis(self):
        self.rets = self.orders
        return self.orders


@pd.api.extensions.register_dataframe_accessor("backtrader")
@pd.api.extensions.register_series_accessor("backtrader")
class BackTrader(Worker):
    """Backtester is a staff dedicated for run backtest on a dataset"""

    def _make_available(self, spt: int):
        if self.is_frame and not 'close' in data.columns:
            raise BackTesterError('run', 'Your data should at least have a column named close')
        if self.type_ == Worker.CS:
            raise BackTesterError('run', 'Cross section data cannot be used to run backtest')
        
        required_col = ['open', 'high', 'low']
        if self.is_frame:
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
        
        data = data.apply(lambda x: x * spt if x.name != 'volume' and x.name != 'openinterest' 
            else x / spt)

    def run(
        self, 
        strategy: bt.Strategy = None, 
        cash: float = 1000000,
        spt: int = 1,
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
        indicators: bt.Indicator or list, a indicator or a list of them
        analyzers: bt.Analyzer or list, an analyzer or a list of them
        observers: bt.Observer or list, a observer or a list of them
        coc: bool, to set whether cheat on close
        image_path: str, path to save backtest image
        data_path: str, path to save backtest data
        show: bool, whether to show the result
        """
        
        data = self.data.copy()
        self._make_available(spt)
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
        cerebro.broker.setcash(cash * spt)
        if coc:
            cerebro.broker.set_coc(True)
        
        # add data
        if self.type_ == Worker.PN:
            datanames = data.index.levels[1].to_list()
        else:
            datanames = ['data']
        for dn in datanames:
            d = data.loc(axis=0)[:, dn].droplevel(1) if self.type_ == Worker.PN else data
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
        self._make_available(spt)
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
        cerebro.broker.setcash(cash * spt)
        if coc:
            cerebro.broker.set_coc(True)

        # add data
        if self.type_ == Worker.PN:
            datanames = data.index.levels[1].to_list()
        else:
            datanames = ['data']
        for dn in datanames:
            d = data.loc(axis=0)[:, dn].droplevel(1) if self.type_ == Worker.PN else data
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


if __name__ == "__main__":
    data = pd.Series(np.random.rand(100), index=pd.MultiIndex.from_product(
        [pd.date_range('20200101', periods=20, freq='3d'), list('abcde')]))
    ret = pd.Series(np.random.rand(300), index=pd.MultiIndex.from_product(
        [pd.date_range('20200101', periods=60), list('abcde')]))
    port = data.groupby(level=0).apply(pd.qcut, labels=False, q=2) + 1
    position = pd.Series(np.random.rand(10), index=pd.MultiIndex.from_tuples([
        (pd.to_datetime('20200101'), 'a'),
        (pd.to_datetime('20200101'), 'b'),
        (pd.to_datetime('20200102'), 'c'),
        (pd.to_datetime('20200102'), 'a'),
        (pd.to_datetime('20200103'), 'd'),
        (pd.to_datetime('20200103'), 'e'),
        (pd.to_datetime('20200104'), 'a'),
        (pd.to_datetime('20200104'), 'e'),
        (pd.to_datetime('20200105'), 'b'),
        (pd.to_datetime('20200105'), 'e'),
    ]))
    # print(data.relocator.profit(ret))
    print(position.relocator.turnover(side='sell'))
    