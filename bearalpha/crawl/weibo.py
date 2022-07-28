import re
import time
import random
import pandas as pd
from urllib.parse import quote
from ..tools import *
from ..core import *

class WeiboSearch:
    '''A search crawler engine for weibo
    ====================================

    sample usage:
    >>> import bearalpha as ba
    >>> result = ba.WeiboSearch.search("keyword")
    '''

    __base = "https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D1%26q%3D{}&page_type=searchall&page={}"

    @classmethod
    def _get_content(cls, url, headers):

        def _parse(mblog):
            blog = {
                "created_at": mblog["created_at"],
                "text": re.sub(r'<(.*?)>', '', mblog['text']),
                "id": mblog["id"],
                "link": f"https://m.weibo.cn/detail/{mblog['id']}",                    
                "source": mblog["source"],
                "username": mblog["user"]["screen_name"],
                "reposts_count": mblog["reposts_count"],
                "comments_count": mblog["comments_count"],
                "attitudes_count": mblog["attitudes_count"],
                "isLongText": mblog["isLongText"],
            }
            if blog["isLongText"]:
                headers = {
                    "Referer": f"https://m.weibo.cn/detail/{blog['id']}",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15"
                }
                resp = Request(f"https://m.weibo.cn/statuses/extend?id={blog['id']}", headers=headers).get().json
                blog["full_text"] = resp["data"]["longTextContent"]
            return blog

        # First try to get resources
        res = Request(url, headers=headers).get().json
        # if it is end
        if res.get("msg"):
            return False

        # if it contains cards
        cards = res["data"]["cards"]
        blogs = []
        for card in cards:
            # find 'mblog' tag and append to result blogs
            mblog = card.get("mblog")
            card_group = card.get("card_group")
            if card.get("mblog"):
                blog = _parse(mblog)
                blogs.append(blog)
            elif card_group:
                for cg in card_group:
                    mblog = cg.get("mblog")
                    if mblog:
                        blog = _parse(mblog)
                        blogs.append(blog)
        return blogs
    
    @classmethod
    def _get_full(cls, keyword: str):
        page = 1
        result = []
        headers = {
            "Referer": f"https://m.weibo.cn/search?containerid=100103type%3D1%26q%3D{quote(keyword, 'utf-8')}",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
            }
        CONSOLE.log(f"[red]Start in keyword: {keyword}[/red]")
        while True:
            CONSOLE.print(f"Getting [blue]{keyword}[/blue], currently at page: [blue]{page}[/blue] ... ")
            url = cls.__base.format(keyword, page)
            blogs = cls._get_content(url, headers)
            if not blogs:
                break
            result.extend(blogs)
            page += 1
            time.sleep(random.randint(5, 8))
        CONSOLE.log(f"[green]Finished in keyword: {keyword}!")
        return result
    
    @classmethod
    def _get_assigned(cls, keyword: str, pages: int):
        result = []
        CONSOLE.log(f"[red]Start in keyword: {keyword}")
        headers = {
            "Referer": f"https://m.weibo.cn/search?containerid=100103type%3D1%26q%3D{quote(keyword, 'utf-8')}",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
            }
        for page in track(range(1, pages+1)):
            CONSOLE.print(f"Getting [blue]{keyword}[/blue], currently at page: [blue]{page}[/blue] ... ")
            url = cls.__base.format(keyword, page)
            blogs = cls._get_content(url, headers)
            result.extend(blogs)
            time.sleep(random.randint(5, 8))
        CONSOLE.log(f"[green]Finished in keyword: {keyword}!")
        return result          
    
    @classmethod
    @Cache(prefix='weibosearch_search', expire_time=604800)
    def search(cls, keyword: str, pages: int = -1):
        """Search for the keyword
        --------------------------
        
        keyword: str, keyword
        pages: int, how many pages you want to get, default -1 to all pages
        """

        keyword = keyword.replace('#', '%23')
        if pages == -1:
            result = cls._get_full(keyword)
        else:
            result = cls._get_assigned(keyword, pages)
        result = pd.DataFrame(result)
        return result


class HotTopic:
    """A Second Level Crawler for Hot Topic
    ========================================

    sample usage:
    >>> import bearalpha as ba
    >>> result = ba.search('keyword')
    """

    __list = "https://google-api.zhaoyizhe.com/google-api/index/mon/list"
    __search = "https://google-api.zhaoyizhe.com/google-api/index/mon/sec?isValid=ads&keyword={}"
    __trend = "https://google-api.zhaoyizhe.com/google-api/index/superInfo?keyword={}"
    
    @classmethod
    @Cache(prefix='weibo_search', expire_time=604800)
    def search(cls, keyword: str = None, date: str = None):
        if keyword is None and date is None:
            url = cls.__list
        elif keyword is None and date is not None:
            url = cls.__search.format(date)
        elif keyword is not None and date is None:
            url = cls.__search.format(keyword)
        result = Request(url).get().json
        data = result["data"]
        data = pd.DataFrame(data)
        data = data.drop("_id", axis=1)
        return data

    @classmethod
    @Cache(prefix='weibo_trend', expire_time=604800)
    def trend(cls, keyword: str):
        url = cls.__trend.format(keyword)
        result = Request(url).get().json
        data = pd.DataFrame(map(lambda x: x['value'], result), 
            columns=['datetime', 'hot', 'tag']).set_index('datetime')
        return data

    @classmethod
    @Cache(prefix='weibo_trend_history', expire_time=604800)
    def trend_history(cls, keyword: str, freq: str = '3m'):
        if freq not in ['1h', '24h', '1m', '3m']:
            raise ValueError('Freq parameter must be in ["1h", "24h', "1m", "3m]")
        if freq.endswith('h'):
            freq += 'our'
        elif freq.endswith('m'):
            freq += 'onth'
        url = "https://data.weibo.com/index/ajax/newindex/searchword"
        data = {
            "word": f"{keyword}"
        }
        headers = {
            "Host": "data.weibo.com",
            "Origin": "https://data.weibo.com",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x18001041) NetType/WIFI Language/zh_CN",
            "Content-Length": "23",
            "Referer": "https://data.weibo.com/index/newindex?visit_type=search"
        }
        html = Request(url, data=data, headers=headers).post().soup
        res = html.find_all('li')
        wids = [(r.attrs["wid"].strip(r'\"'), eval('"' + r.attrs["word"].replace(r'\"', '') + '"')) for r in res]

        url = "https://data.weibo.com/index/ajax/newindex/getchartdata"
        results = []
        for wid in wids:
            post_params = {
                "wid": wid[0],
                "dateGroup": freq
            }
            res = Request(url, data=post_params, headers=headers).post().json
            data = res["data"]
            index = data[0]["trend"]['x']
            index = list(map(lambda x: x.replace("月", '-').replace("日", ''), index))
            volume = data[0]["trend"]['s']
            result = pd.Series(volume, index=index, name=wid[1])
            results.append(result)
        results = pd.concat(results, axis=1)
        return results