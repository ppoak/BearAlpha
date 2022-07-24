from h11 import Data
from bearalpha import *


class WeiboSearch:

    @classmethod
    def search(cls, keyword: str, pages: int = -1) -> DataFrame: 
        """Search for the keyword
        --------------------------
        
        keyword: str, keyword
        pages: int, how many pages you want to get, default -1 to all pages
        """


class HotTopic:

    @classmethod
    def search(cls, keyword: str = None, date: str = None) -> DataFrame: ...

    @classmethod
    def trend_history(cls, keyword: str, freq: str = '3m') -> DataFrame: ...

    @classmethod
    def trend(cls, keyword: str) -> DataFrame: ...
