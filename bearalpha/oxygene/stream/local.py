import pandas as pd
from ..base import *


class Local(Loader):
    
    def read(self, path: str, **kwargs) -> None:
        extension = path.split('.')[-1]
        if extension == 'xlsx' or extension == 'xls':
            data = pd.read_excel(path, **kwargs)
        elif extension == 'csv':
            data = pd.read_csv(path, **kwargs)
        elif extension == 'par' or extension == 'parquet':
            data = pd.read_parquet(path, **kwargs)
        self.data = data

    def write(self):
        self.data.sqliter.to_sql(table=self.table, database=self.database)
        
    def postprocess(self, **kwargs):
        self.data.sqliter.add_index(table=self.table, database=self.database, **kwargs['add_index'])
