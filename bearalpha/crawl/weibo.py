import pandas as pd
from ..tools import *
from ..core import *


class HotTopic:
    """A Second Level Crawler for Hot Topic
    ========================================

    sample usage:
    >>> from search import HotTopic
    >>> search = HotTopic("周深")
    >>> result = search.run()
    >>> result.to_excel("周深-热搜话题.xlsx", index=False)
    """

    __list = "https://google-api.zhaoyizhe.com/google-api/index/mon/list"
    __search = "https://google-api.zhaoyizhe.com/google-api/index/mon/sec?isValid=ads&keyword={}"
    __trend = "https://google-api.zhaoyizhe.com/google-api/index/superInfo?keyword={}"
    
    @classmethod
    def search(cls, keyword: str = None, date: str = None):
        if keyword is None and date is None:
            url = cls.__list
        elif keyword is None and date is not None:
            url = cls.__search.format(date)
        elif keyword is not None and date is None:
            url = cls.__search.formate(keyword)
        result = Request(url).get().json
        data = result["data"]
        data = pd.DataFrame(data)
        data = data.drop("_id", axis=1)
        return data

    @classmethod
    def trend(cls, keyword: str):
        url = cls.__trend.format(keyword)
        result = Request(url).get().json
        data = pd.DataFrame(map(lambda x: x['value'], result), 
            columns=['datetime', 'hot', 'tag']).set_index('datetime')
        return data
    