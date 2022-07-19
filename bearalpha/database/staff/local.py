import pandas as pd
from ..tools import Loader


class Local(Loader):
    
    def __init__(self, config) -> None:
        super().__init__(config)
        self.path = config['path']
        self.addindex = config.get('addindex', None)
        self.read_config = config.get('read_config', {})
    
    def read(self) -> None:
        path = self.path
        read_config = self.read_config

        extension = path.split('.')[-1]
        if extension == 'xlsx' or extension == 'xls':
            data = pd.read_excel(path, **read_config)
        elif extension == 'csv':
            data = pd.read_csv(path, **read_config)
        elif extension == 'par' or extension == 'parquet':
            data = pd.read_parquet(path, **read_config)
            
        self.data = data
    
    def write(self):
        self.data.databaser.to_sql(table=self.table, database=self.database)
    
    def __call__(self, *args, processor = None, **kwargs):
        self.read()
        if processor is not None:
            self.data = processor(self.data, *args, **kwargs)
        self.write()
        self.post()

