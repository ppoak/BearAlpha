import datetime
import backtrader as bt
from bearalpha import *


class Strategy(bt.Strategy):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""

    def notify_order(self, order: bt.Order):
        """order notification"""

    def notify_trade(self, trade):
        """trade notification"""


class Indicator(bt.Indicator):
    
    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""
    

class Analyzer(bt.Analyzer):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""


class Observer(bt.Observer):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""

class OrderTable(Analyzer):

    def notify_order(self, order): ...
    def get_analysis(self) -> DataFrame: ...
