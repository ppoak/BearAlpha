import akshare as ak
import baostock as bs
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

stock_database = create_engine("mysql+pymysql://kali:kali123@127.0.0.1/stock?charset=utf8", poolclass=NullPool,
                connect_args={"charset": "utf8", "connect_timeout": 10})
fund_database = create_engine("mysql+pymysql://kali:kali123@127.0.0.1/fund?charset=utf8", poolclass=NullPool,
                connect_args={"charset": "utf8", "connect_timeout": 10})
factor_database = create_engine("mysql+pymysql://kali:kali123@127.0.0.1/factor?charset=utf8", poolclass=NullPool,
                connect_args={"charset": "utf8", "connect_timeout": 10})
