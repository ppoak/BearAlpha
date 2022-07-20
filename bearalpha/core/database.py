import datetime
import pandas as pd
import sqlalchemy as sql
from .crawl import Cache
from ..tools import *

class DataBase:
    def __init__(self, database: sql.engine.Engine) -> None:
        self.today = datetime.datetime.today()
        self.engine = database
        
    def __query(self, table: str, start: str, end: str, date_col: str, 
        code: 'str | list', code_col: str, fields: 'list | str', 
        index_col: 'str | list', and_: 'list | str', or_: 'str | list'):
        start = start or '20070104'
        end = end or self.today
        start = str2time(start)
        end = str2time(end)
        code = item2list(code)
        fields = item2list(fields)
        index_col = item2list(index_col)
        and_ = item2list(and_)
        or_ = item2list(or_)

        if fields:
            fields = set(fields).union(set(index_col))
            fields = ','.join(fields)
        else:
            fields = '*'

        query = f'select {fields} from {table}'
        query += f' where ' if any([date_col, code, and_, or_]) else ''
        
        if date_col:
            query += f' ( {date_col} between "{start}" and "{end}" )'
            
        if code:
            query += ' and ' if any([date_col]) else ''
            query += '(' + ' or '.join([f'{code_col} like "%%{c}%%"' for c in code]) + ')'
        
        if and_:
            query += ' and ' if any([date_col, code]) else ''
            query += '(' + ' and '.join(and_) + ')'
        
        if or_:
            query += ' and ' if any([date_col, code, and_]) else ''
            query += '(' + ' or '.join(or_) + ')'
        
        return query

    def get(self, 
        table: str,
        start: str, 
        end: str,
        date_col: str,
        code: 'str | list',
        code_col: str,
        fields: list, 
        index_col: 'str | list',
        and_: 'str | list', 
        or_: 'str | list',
        ) -> pd.DataFrame:
        query = self.__query(
            table,
            start,
            end,
            date_col,
            code,
            code_col,
            fields,
            index_col,
            and_,
            or_
        )
        data = pd.read_sql(query, self.engine, parse_dates=date_col)
        if index_col is not None:
            data = data.set_index(index_col)
        if fields is not None and isinstance(fields, str):
            data = data.iloc[:, 0]
        if isinstance(code, str):
            data = data.droplevel(1)
        return data


class Loader:

    def __init__(self, config) -> None:
        self.table = config['table']
        self.database = config['database']
        self.addindex = config.get('addindex', None)
        if isinstance(self.database, str):
            self.database = sql.engine.create_engine(self.database)
    
    def post(self):
        if self.addindex is not None:
            with self.database.connect() as connect:
                if self.database.name == 'sqlite':
                    idxnames = connect.execute(f'SELECT name FROM sqlite_master WHERE type ' 
                        f'= "index" and tbl_name = "{self.table}"').fetchall()
                elif self.database.name == 'mysql':
                    idxnames = connect.execute(f'SHOW INDEX FROM {self.table}').fetchall()
                idxnames = list(map(lambda x: x[0], idxnames))
                for idxname, column in self.addindex.items():
                    if idxname not in idxnames:
                        sqlcol = str(column).replace("'", "`")
                        connect.execute(f'CREATE INDEX `{idxname}` on {self.table} ({sqlcol})')

    def __call__(self):
        raise NotImplementedError
