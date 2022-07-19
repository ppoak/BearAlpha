import quool as ql
from ..tools import Loader

class TuShare(Loader):
    
    def __init__(self, config) -> None:
        super().__init__(config)
        self.func = config['func']
        self.args = config.get('args', {})

    def write(self):
        for arg in ql.Track(list(self.args)):
            data = None
            while data is None:
                try:
                    data = self.func(*arg)
                except:
                    pass
            data.databaser.to_sql(table=self.table, database=self.database)
    
    def __call__(self):
        self.write()
        self.post()
