import datetime
import pandas as pd
import backtrader as bt
from ..tools import *


class FrameWorkError(Exception):
    def __init__(self, func: str, hint: str) -> None:
        self.func = func
        self.hint = hint
    
    def __str__(self) -> str:
        return f'[-] <{self.func}> {self.hint}'


class Worker(object):
    TSFR = 1
    CSFR = 2
    PNFR = 3
    TSSR = 4
    CSSR = 5
    PNSR = 6
    
    def __init__(self, data: 'pd.DataFrame | pd.Series'):
        self.data = data
        self._validate()
    
    @staticmethod
    def series2frame(data: pd.Series, name: str = None):
        return data.to_frame(name=name or 'frame')
    
    @staticmethod
    def frame2series(data: pd.DataFrame, name: str = None):
        name = name or data.columns[0]
        data = data.iloc[:, 0].copy()
        data.name = name
        return data
    
    @staticmethod
    def ists(data: 'pd.DataFrame | pd.Series'):
        return not isinstance(data.index, pd.MultiIndex) and isinstance(data.index, pd.DatetimeIndex)
    
    @staticmethod
    def iscs(data: 'pd.DataFrame | pd.Series'):
        return not isinstance(data.index, pd.MultiIndex) and not isinstance(data.index, pd.DatetimeIndex)
    
    @staticmethod
    def ispanel(data: 'pd.DataFrame | pd.Series'):
        return isinstance(data.index, pd.MultiIndex) and len(data.index.levshape) >= 2 \
                and isinstance(data.index.levels[0], pd.DatetimeIndex)
    
    @staticmethod
    def isframe(data: 'pd.DataFrame | pd.Series'):
        return isinstance(data, pd.DataFrame)
    
    @staticmethod
    def isseries(data: 'pd.DataFrame | pd.Series'):
        return isinstance(data, pd.Series)

    def _validate(self):

        is_frame = self.isframe(self.data)
        is_series = self.isseries(self.data)
        
        if is_frame and self.data.columns.size == 1:
            is_frame = False
            is_series = True
            self.data = self.frame2series(self.data)
            
        if self.data.empty:
            raise ValueError('[!] Dataframe or Series is empty')

        is_ts = self.ists(self.data)
        is_cs = self.iscs(self.data)
        is_panel = self.ispanel(self.data)
        
        if is_ts and is_frame:
            self.type_ = Worker.TSFR
        elif is_cs and is_frame:
            self.type_ = Worker.CSFR
        elif is_panel and is_frame:
            self.type_ = Worker.PNFR
        elif is_ts and is_series:
            self.type_ = Worker.TSSR
        elif is_cs and is_series:
            self.type_ = Worker.CSSR
        elif is_panel and is_series:
            self.type_ = Worker.PNSR
        else:
            raise ValueError("Your dataframe or series seems not supported in our framework")
 
    def _flat(self, datetime, asset, indicator):
        
        data = self.data.copy()
        check = (not isinstance(datetime, slice), not isinstance(asset, slice), not isinstance(indicator, slice))

        if self.type_ == Worker.PNFR:
            # is a panel and is a dataframe
            if check == (False, False, False):
                raise ValueError('Must assign at least one of dimension')
            elif check == (False, True, True):
                return data.loc[(datetime, asset), indicator].droplevel(1)
            elif check == (True, False, True):
                return data.loc[(datetime, asset), indicator].droplevel(0)
            elif check == (True, True, False):
                return data.loc[(datetime, asset), indicator]
            elif check == (True, False, False):
                return data.loc[(datetime, asset), indicator].droplevel(0)
            elif check == (False, True, False):
                return data.loc[(datetime, asset), indicator].droplevel(1)
            elif check == (False, False, True):
                return data.loc[(datetime, asset), indicator].unstack(level=1)
            elif check == (True, True, True):
                print('[!] single value was selected')
                return data.loc[(datetime, asset), indicator]
                
        elif self.type_ == Worker.PNSR:
            # is a panel and is a series
            if (check[-1] or not any(check)):
                if check[-1]:
                    print("[!] Your data is not a dataframe, indicator will be ignored")
                return data.unstack()
            elif check[1]:
                return data.loc[(datetime, asset)].unstack()
            elif check[0]:
                return data.loc[(datetime, asset)]
                
        # not a panel and is a series
        elif self.type_ == Worker.TSSR:
            return data.loc[datetime]
        elif self.type_ == Worker.CSSR:
            return data.loc[asset]
        # not a panel and is a dataframe
        elif self.type_ == Worker.CSFR:
            return data.loc[(asset, indicator)]
        elif self.type_ == Worker.TSFR:
            return data.loc[(datetime, indicator)]


class Strategy(bt.Strategy):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""
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
        """order notification"""
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
        """trade notification"""
        if not trade.isclosed:
            # trade not closed, skip
            return
        # else, log it
        self.log(f'Gross Profit: {trade.pnl:.2f}, Net Profit {trade.pnlcomm:.2f}')


class Indicator(bt.Indicator):
    
    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""
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
        """Logging function"""
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
        """Logging function"""
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
        self.orders = []

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.orders.append([
                    self.data.datetime.date(0),
                    order.info.get('name', 'data'), order.executed.size, 
                    order.executed.price, 'BUY']
                )
            elif order.issell():
                self.orders.append([
                    self.data.datetime.date(0),
                    order.info.get('name', 'data'), order.executed.size, 
                    order.executed.price, 'SELL']
                )
        
    def get_analysis(self):
        self.rets = pd.DataFrame(self.orders, columns=['datetime', 'asset', 'size', 'price', 'direction'])
        self.rets = self.rets.set_index('datetime')
        return self.orders


def async_job(
    jobs: list, 
    func: ..., 
    args: tuple = (), 
    processors: int = 4, 
    callback: ... = None, 
    kwargs: dict = {}
):
    import multiprocessing
    context = multiprocessing.get_context('fork')
    pool = context.Pool(processes=processors)
    result = {}
    for job in jobs:
        result[job] = pool.apply_async(func, args=(job,) + args, kwds=kwargs, callback=callback)
    pool.close()
    pool.join()
    for k, v in result.items():
        result[k] = v.get()
    return result


if __name__ == "__main__":
    pass
