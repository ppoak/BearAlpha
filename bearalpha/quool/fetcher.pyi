import sqlalchemy as sql
from bearalpha import *


class Filer(quool.base.Worker):

    def to_excel(
        self, 
        path, 
        perspective: str = 'indicator', 
        **kwargs
    ) -> None: ...

    @staticmethod
    def read_csv_directory(
        path, 
        perspective: str, 
        name_pattern: str = None, 
        **kwargs
    ) -> 'DataFrame | Series':
        '''A enhanced function for reading files in a directory to a panel DataFrame
        ----------------------------------------------------------------------------

        path: path to the directory
        perspective: 'datetime', 'asset', 'indicator'
        name_pattern: pattern to match the file name, which will be extracted as index
        kwargs: other arguments for pd.read_csv

        **note: the name of the file in the directory will be interpreted as the 
        sign(column or index) to the data, so set it to the brief one
        '''

    @staticmethod
    def read_excel_directory(
        path, 
        perspective: str, 
        name_pattern: str = None, 
        **kwargs
    ) -> 'DataFrame | Series':
        '''A enhanced function for reading files in a directory to a panel DataFrame
        ----------------------------------------------------------------------------

        path: path to the directory
        perspective: 'datetime', 'asset', 'indicator'
        kwargs: other arguments for pd.read_excel

        **note: the name of the file in the directory will be interpreted as the 
        sign(column or index) to the data, so set it to the brief one
        '''

    def to_parquet(
        self, 
        path, 
        append: bool = True, 
        compression: str = 'snappy', 
        index: bool = None, 
        **kwargs
    ) -> None:
        '''A enhanced function enables parquet file appending to existing file
        -----------------------------------------------------------------------
        
        path: path to the parquet file
        append: whether to append to the existing file
        compression: compression method
        index: whether to write index to the parquet file
        '''


class Databaser(quool.base.Worker):
    ...


class Sqliter(Databaser):

    def to_sql(
        self, 
        table: str, 
        database: 'sql.engine.base.Engine | str', 
        index: bool = True,
        on_duplicate: str = "update", 
        chunksize: int = 2000
    ) -> None:
        """Save current dataframe to database
        ---------------------------------------

        table: str, table to insert data;
        database: Sqlalchemy Engine object
        index: bool, whether to include index in the database
        on_duplicate: str, optional {"update", "replace", "ignore"}, default "update" specified the way to update
            "update": "INSERT ... ON DUPLICATE UPDATE ...", 
            "replace": "REPLACE ...",
            "ignore": "INSERT IGNORE ..."
        chunksize: int, size of records to be inserted each time;
        """


    @staticmethod
    def add_index(
        table: str, 
        database: 'sql.engine.base.Engine | str', 
        **indexargs
    ) -> None: ...

    @staticmethod
    def formattime(
        database: 'sql.engine.Engine | str', 
        table: str
    ) -> None: ...


class Mysqler(Databaser):

    def to_sql(
        self,
        table: str, 
        database: 'sql.engine.base.Engine | str', 
        index: bool = True,
        on_duplicate: str = "update", 
        chunksize: int = 2000
    ) -> None:
        """Save current dataframe to database

        table: str, table to insert data;
        database: Sqlalchemy Engine object
        index: bool, whether to include index in the database
        on_duplicate: str, optional {"update", "replace", "ignore"}, default "update" specified the way to update
            "update": "INSERT ... ON DUPLICATE UPDATE ...", 
            "replace": "REPLACE ...",
            "ignore": "INSERT IGNORE ..."
        chunksize: int, size of records to be inserted each time;
        """

    @staticmethod
    def add_index(
        table: str, 
        database: 'sql.engine.base.Engine | str', 
        **indexargs
    ) -> None: ...
