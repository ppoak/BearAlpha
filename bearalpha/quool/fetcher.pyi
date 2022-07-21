import sqlalchemy as sql
from bearalpha import *


class Filer(Worker):

    def to_multisheet_excel(
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
    ) -> 'DataFrame | Series': ...

    @staticmethod
    def read_excel_directory(
        path, 
        perspective: str, 
        name_pattern: str = None, 
        **kwargs
    ) -> 'DataFrame | Series': ...

    def to_parquet(
        self, 
        path, 
        append: bool = True, 
        compression: str = 'snappy', 
        index: bool = None, 
        **kwargs
    ) -> None: ...


class Databaser(Worker):
    ...


class Sqliter(Databaser):

    def to_sql(
        self, 
        table: str, 
        database: 'sql.engine.base.Engine | str', 
        index: bool = True,
        on_duplicate: str = "update", 
        chunksize: int = 2000
    ) -> None: ...

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
    ) -> None: ...

    @staticmethod
    def add_index(
        table: str, 
        database: 'sql.engine.base.Engine | str', 
        **indexargs
    ) -> None: ...
