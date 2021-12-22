import pandas as pd
from rqdatac import all_instruments

def get_stock_pool(date: str) -> pd.DataFrame:
    '''获取可投股票池
    ---------------------------
    
    参数:
    -----------
    date: str, 给定获取信息的日期
    
    返回值:
    -----------
    
    DataFrame, 按照以下顺序排列的列
    标的代码(0), 标的名称(symble 1), 行业代码(2), 
    行业名称(3), 板块代码(4), 板块名称(5)
    '''
    stock_pool = all_instruments(type='CS', date=date)
    stock_pool = stock_pool[stock_pool["status"]=="Active"]
    stock_pool = stock_pool.loc[:, ["order_book_id", "symbol", "industry_code", 
                                    "industry_name","sector_code", "sector_code_name"]]
    return stock_pool