import pandas as pd

def get_ret(filepath: str, window: int, method: str):
    pd.read_csv(filepath, index_col=0, parse_dates=True)