import os
import re
import sys
import pandas as pd
import sqlalchemy as sql
from ..tools import *


@pd.api.extensions.register_dataframe_accessor("filer")
@pd.api.extensions.register_series_accessor("filer")
class Filer(Worker):
    
    def to_multisheet_excel(self, path, perspective: str = 'indicator', **kwargs):
        if self.type_ == Worker.PN:
            if isinstance(path, str):
                writer = pd.ExcelWriter(path)
            else:
                writer = path
            if perspective == 'indicator':
                if self.is_frame:
                    for column in self.data.columns:
                        self.data[column].unstack(level=1).to_excel(writer, sheet_name=str(column), **kwargs)
                else:
                    self.data.unstack(level=1).to_excel(writer, sheet_name=str(self.data.name), **kwargs)

            elif perspective == 'datetime':
                for date in self.data.index.get_level_values(0).unique():
                    self.data.loc[date].to_excel(writer, sheet_name=str(date), **kwargs)
                
            elif perspective =='asset':
                if not self.is_frame:
                    for asset in self.data.index.get_level_values(1).unique():
                        self.data.loc[(slice(None), asset)].to_excel(writer, sheet_name=str(asset), **kwargs)
                else:
                    for asset in self.data.index.get_level_values(1).unique():
                        self.data.loc[(slice(None), asset), :].to_excel(writer, sheet_name=str(asset), **kwargs)
        else:
            self.data.to_excel(path, **kwargs)
        
        if isinstance(path, str):
            writer.close()

    @staticmethod
    def read_excel(path, perspective: str = None, **kwargs):
        '''A dummy function of pd.read_csv, which provide multi sheet reading function'''
        if perspective is None:
            return pd.read_excel(path, **kwargs)
        
        sheets_dict = pd.read_excel(path, sheet_name=None, **kwargs)
        datas = []
        if perspective == "indicator":
            for indicator, data in sheets_dict.items():
                data = data.stack()
                data.name = indicator
                datas.append(data)
            datas = pd.concat(datas, axis=1)

        elif perspective == "asset":
            for asset, data in sheets_dict.items():
                data.index = pd.MultiIndex.from_product([data.index, [asset]])
                datas.append(data)
            datas = pd.concat(datas)
            datas = data.sort_index()

        elif perspective == "datetime":
            for datetime, data in sheets_dict.items():
                data.index = pd.MultiIndex.from_product([[datetime], data.index])
                datas.append(data)
            datas = pd.concat(datas)

        else:
            raise ValueError('perspective must be in one of datetime, indicator or asset')
        
        return datas

    @staticmethod
    def read_csv_directory(path, perspective: str, name_pattern: str = None, **kwargs):
        '''A enhanced function for reading files in a directory to a panel DataFrame
        ----------------------------------------------------------------------------

        path: path to the directory
        perspective: 'datetime', 'asset', 'indicator'
        name_pattern: pattern to match the file name, which will be extracted as index
        kwargs: other arguments for pd.read_csv

        **note: the name of the file in the directory will be interpreted as the 
        sign(column or index) to the data, so set it to the brief one
        '''
        files = sorted(os.listdir(path))
        datas = []
        
        if perspective == "indicator":
            if name_pattern is None:
                name_pattern = r'.*'
            for file in files:
                basename = os.path.splitext(file)[0]
                name = re.findall(name_pattern, basename)[0]
                data = pd.read_csv(os.path.join(path, file), **kwargs)
                data = data.stack()
                data.name = name
                datas.append(data)
            datas = pd.concat(datas, axis=1).sort_index()

        elif perspective == "asset":
            if name_pattern is None:
                name_pattern = r'[a-zA-Z\d]{6}\.[a-zA-Z]{2}|[a-zA-Z]{0,2}\..{6}'
            for file in files:
                basename = os.path.splitext(file)[0]
                name = re.findall(name_pattern, basename)[0]
                data = pd.read_csv(os.path.join(path, file), **kwargs)
                data.index = pd.MultiIndex.from_product([data.index, [name]])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
            
        elif perspective == "datetime":
            if name_pattern is None:
                name_pattern = r'\d{4}[./-]\d{2}[./-]\d{2}|\d{4}[./-]\d{2}[./-]\d{2}\s?\d{2}[:.]\d{2}[:.]\d{2}'
            for file in files:
                basename = os.path.splitext(file)[0]
                name = re.findall(name_pattern, basename)[0]
                data = pd.read_csv(os.path.join(path, file), **kwargs)
                data.index = pd.MultiIndex.from_product([pd.to_datetime([name]), data.index])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
        
        return datas

    @staticmethod
    def read_excel_directory(path, perspective: str, name_pattern: str = None, **kwargs):
        '''A enhanced function for reading files in a directory to a panel DataFrame
        ----------------------------------------------------------------------------

        path: path to the directory
        perspective: 'datetime', 'asset', 'indicator'
        kwargs: other arguments for pd.read_excel

        **note: the name of the file in the directory will be interpreted as the 
        sign(column or index) to the data, so set it to the brief one
        '''
        files = os.listdir(path)
        datas = []
        
        if perspective == "indicator":
            if name_pattern is None:
                name_pattern = r'.*'
            for file in files:
                basename = os.path.splitext(file)[0]
                name = re.findall(name_pattern, basename)[0]
                data = pd.read_excel(os.path.join(path, file), **kwargs)
                data = data.stack()
                data.name = name
                datas.append(data)
            datas = pd.concat(datas, axis=1).sort_index()

        elif perspective == "asset":
            if name_pattern is None:
                name_pattern = r'[a-zA-Z\d]{6}\.[a-zA-Z]{2}|[a-zA-Z]{0,2}\..{6}'
            for file in files:
                basename = os.path.splitext(file)[0]
                name = re.search(name_pattern, basename).group()
                data = pd.read_excel(os.path.join(path, file), **kwargs)
                data.index = pd.MultiIndex.from_product([data.index, [name]])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
            
        elif perspective == "datetime":
            if name_pattern is None:
                name_pattern = r'\d{4}[./-]\d{2}[./-]\d{2}|\d{4}[./-]\d{2}[./-]\d{2}\s?\d{2}[:.]\d{2}[:.]\d{2}'
            for file in files:
                basename = os.path.splitext(file)[0]
                name = re.search(name_pattern, basename).group()
                data = pd.read_excel(os.path.join(path, file), **kwargs)
                data.index = pd.MultiIndex.from_product([pd.to_datetime([name]), data.index])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
        
        return datas

    def to_parquet(self, path, append: bool = True, 
        compression: str = 'snappy', index: bool = None, **kwargs):
        '''A enhanced function enables parquet file appending to existing file
        -----------------------------------------------------------------------
        
        path: path to the parquet file
        append: whether to append to the existing file
        compression: compression method
        index: whether to write index to the parquet file
        '''
        import pyarrow

        if append:
            original_data = pd.read_parquet(path)
            data = pd.concat([original_data, self.data])
            data = pyarrow.Table.from_pandas(data)
            with pyarrow.parquet.ParquetWriter(path, data.schema, compression=compression) as writer:
                writer.write_table(table=data)
        else:
            self.data.to_parquet(path, compression=compression, index=index, **kwargs)
  
@pd.api.extensions.register_dataframe_accessor("databaser")
@pd.api.extensions.register_series_accessor("databaser")
class Databaser(Worker):

    def __sql_cols(self, data, usage="sql"):
        '''internal usage: get sql columns from dataframe df'''
        cols = tuple(data.columns)
        if usage == "sql":
            cols_str = str(cols).replace("'", "`")
            if len(data.columns) == 1:
                cols_str = cols_str[:-2] + ")"  # to process dataframe with only one column
            return cols_str
        elif usage == "format":
            base = "'%%(%s)s'" % cols[0]
            for col in cols[1:]:
                base += ", '%%(%s)s'" % col
            return base
        elif usage == "values":
            base = "%s=VALUES(%s)" % (cols[0], cols[0])
            for col in cols[1:]:
                base += ", `%s`=VALUES(`%s`)" % (col, col)
            return base
        elif usage == "excluded":
            base = "%s=excluded.%s" % (cols[0], cols[0])
            for col in cols[1:]:
                base += ', `%s`=excluded.`%s`' % (col, col)
            return base

    def to_sql(self, table: str, database: 'sql.engine.base.Engine | str', index: bool = True,
        on_duplicate: str = "update", chunksize: int = 2000):
        """Save current dataframe to database, only support for mysql and sqlite
        ------------------------------------------------------------------------

        table: str, table to insert data;
        database: Sqlalchemy Engine object
        kind: str, optional {"update", "replace", "ignore"}, default "update" specified the way to update
            "update": "INSERT ... ON DUPLICATE UPDATE ...", 
            "replace": "REPLACE ...",
            "ignore": "INSERT IGNORE ..."
        chunksize: int, size of records to be inserted each time;
        """
        # if database is a str connection, just transform it
        if isinstance(database, str):
            database = sql.engine.create_engine(database)
        
        # we should ensure data is in a frame form and no index can be assigned      
        data = self.data.copy()
        if not self.is_frame:
            data = data.to_frame()
        if index:
            if isinstance(self.data.index, pd.MultiIndex):
                index_col = '(`' + '`, `'.join(self.data.index.names) + '`)'
            else:
                index_col = f'(`{self.data.index.name}`)'
            if data.index.has_duplicates:
                CONSOLE.print('[yellow][!][/yellow] Warning: index has duplicates, will be ignored except the first one')
                data = data[~data.index.duplicated(keep='first')]
            data = data.reset_index()

        engine_type = database.name
        # check whether table exists
        if engine_type == "sqlite":
            with database.connect() as conn:
                check = database.execute("SELECT name FROM sqlite_master"
                    " WHERE type='table' AND name='%s'" % table).fetchall()
        elif engine_type == "mysql": 
            with database.connect() as conn:
                check = database.execute("SHOW TABLES LIKE '%s'" % table).fetchall()       
        
        # if table does not exist, create table
        if not check:
            data.to_sql(table, database, index=False)
            if engine_type == "mysql" and index:
                with database.connect() as conn:
                    print(20*'*', index_col, 20*'*')
                    index_col_cp = index_col
                    index_col_cp = index_col_cp[1:-1] # remove ()
                    col_ls = index_col_cp.split(',')
                    skips = ['date', 'period']
                    for col in col_ls:
                        col = col.strip()
                        for substring in skips:
                            if col[1:-1].lower().find(substring) != -1:
                                break
                        else:
                            conn.execute(f"ALTER TABLE `{table}` MODIFY `{col[1:-1]}` {sql.types.CHAR(length=20)}")
                                
                    conn.execute("ALTER TABLE %s ADD INDEX %s" % (table, index_col))
            elif engine_type == "sqlite" and index:
                # unluckily sqlite3 donesn't support alter table, 
                # so we have to drop and recreate
                # https://www.yiibai.com/sqlite/primary-key.html
                with database.connect() as conn:
                    sql_create = conn.execute("SELECT sql FROM sqlite_master WHERE tbl_name = '%s'"
                         % table).fetchall()[0][0]
                    conn.execute("ALTER TABLE %s RENAME TO %s_temp" % (table, table))
                    sql_create_index = sql_create.replace(")", ", PRIMARY KEY %s)" % index_col)
                    conn.execute(sql_create_index)
                    conn.execute("INSERT INTO %s SELECT * FROM %s_temp" % (table, table))
                    conn.execute("DROP TABLE %s_temp" % table)
            return
        
        table = ".".join(["`" + x + "`" for x in table.split(".")])

        data = data.fillna("None")
        data = data.applymap(lambda x: re.sub(r'([\'\"\\])', '\\\g<1>', str(x)))
        cols_str = self.__sql_cols(data)
        for i in range(0, len(data), chunksize):
            # print("chunk-{no}, size-{size}".format(no=str(i/chunksize), size=chunksize))
            tmp = data.iloc[i: i + chunksize]

            if on_duplicate == "replace":
                if engine_type == 'mysql':
                    sql_base = f"REPLACE INTO {table} {cols_str}"
                elif engine_type == 'sqlite':
                    sql_base = f"INSERT OR REPLACE INTO {table} {cols_str}"

            elif on_duplicate == "update":
                sql_base = f"INSERT INTO {table} {cols_str}"
                if engine_type == "mysql":
                    sql_update = f" ON DUPLICATE KEY UPDATE {self.__sql_cols(tmp, 'values')}"
                elif engine_type == "sqlite":
                    sql_update = f" ON CONFLICT DO UPDATE SET {self.__sql_cols(tmp, 'excluded')}"

            elif on_duplicate == "ignore":
                if engine_type == "mysql":
                    sql_base = f"INSERT IGNORE INTO {table} {cols_str}"
                elif engine_type == "sqlite":
                    sql_base = f"INSERT OR IGNORE INTO {table} {cols_str}"

            sql_val = self.__sql_cols(tmp, "format")
            vals = tuple([sql_val % x for x in tmp.to_dict("records")])
            sql_vals = " VALUES ({x})".format(x=vals[0])
            for i in range(1, len(vals)):
                sql_vals += ", ({x})".format(x=vals[i])
            sql_vals = sql_vals.replace("'None'", "NULL")

            sql_main = sql_base + sql_vals
            if on_duplicate == "update":
                sql_main += sql_update

            if sys.version_info.major == 2:
                sql_main = sql_main.replace("u`", "`")
            if sys.version_info.major == 3:
                sql_main = sql_main.replace("%", "%%")
            with database.connect() as conn:
                conn.execute(sql_main)
    
    @staticmethod
    def add_index(table: str, database: 'sql.engine.base.Engine | str', **indexargs):
        # if database is a str connection, just transform it
        if isinstance(database, str):
            database = sql.engine.create_engine(database)

        # to see whether the index already exists
        with database.connect() as connect:
            if database.name == 'sqlite':
                idxnames = connect.execute(f'SELECT name FROM sqlite_master WHERE type ' 
                    f'= "index" and tbl_name = "{table}"').fetchall()
            elif database.name == 'mysql':
                idxnames = connect.execute(f'SHOW INDEX FROM {table}').fetchall()
            idxnames = list(map(lambda x: x[0], idxnames))

        for idxname, arg in indexargs.items():
            if idxname not in idxnames:
                if isinstance(arg, str):
                    sqlcol = str(arg).replace("'", "`")
                    indextype = ''
                elif isinstance(arg, dict):
                    sqlcol = arg['col']
                    indextype = arg.get('type', '')
                with database.connect() as connect:
                    connect.execute(f'CREATE {indextype} INDEX `{idxname}` on {table} ({sqlcol})')


if __name__ == '__main__':
    # fetcher = StockUS("guflrppo3jct4mon7kw13fmv3dsz9kf2")
    # price = fetcher.cn_price('000001.SZ', '20100101', '20200101')
    # print(price)
    from .provider import Stock
    import numpy as np
    con = sql.create_engine('sqlite:///./data.nosync/stock.db')
    Stock.trade_date('20200101', '20220101').databaser.to_sql('test', con)
    # conn = sql.create_engine('mysql+pymysql://windreader:password1@221.226.96.114:10087/wind')
    