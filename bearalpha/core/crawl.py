import os
import time
import json
import random
import pickle
import hashlib
import diskcache
import requests
import pandas as pd
from lxml import etree
from functools import wraps
from bs4 import BeautifulSoup


class Cache(diskcache.Cache):

    def __init__(self, 
        directory: str = None, 
        prefix: str = 'generic', 
        expire_time: float = 3600,
    ):
        if directory is None:
            directory = os.path.join(os.path.split(
                os.path.abspath(__file__))[0] , '..', 'cache')
        self.prefix = prefix
        self.expire_time = expire_time
        super().__init__(directory)
        self.reset('size_limit', int(50e6))
        self.reset('cull_limit', 0)
    
    @staticmethod
    def md5key(func, *args, **kwargs):
        return hashlib.md5(pickle.dumps(f'{func.__name__};{args};{kwargs}')).hexdigest()
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            hash_key = self.md5key(func, *args, **kwargs)
            data = self.get(key=self.prefix + ':' + hash_key)
            if data is not None:
                # get cache successful
                return data
            else:
                # not fund cache,return data will be cache
                result = func(*args, **kwargs)
                self.set(key=self.prefix + ':' + hash_key, value=result, expire=self.expire_time)
                return result
        return wrapper


class Request:

    def __init__(self, url, headers: dict = None, verbose: bool = True, **kwargs):
        self.url = url
        if headers:
            headers.update(self.header)
            self.headers = headers
        else:
            self.headers = self.header
        self.kwargs = kwargs
        self.verbose = verbose
        
    @property
    def header(self):
        ua_list = [
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',
            'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
            ]
        base_header = {
            "User-Agent": random.choice(ua_list),
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'Accept-Language': 'zh-CN,zh;q=0.8'
        }
        return base_header

    def get(self):
        try:
            response = requests.get(self.url, headers=self.headers, **self.kwargs)
            response.raise_for_status()
            self.response = response
            if self.verbose:
                print(f'[+] {self.url} Get Success!')
            return self
        except Exception as e:
            if self.verbose:
                print(f'[-] Error: {e}')

    def post(self):
        try:
            response = requests.post(self.url, headers=self.headers, **self.kwargs)
            response.raise_for_status()        
            self.response = response
            if self.verbose:
                print(f'[+] {self.url} Post Success!')
            return self
        except Exception as e:
            if self.verbose:
                print(f'[-] Error: {e}')

    @property
    def etree(self):
        return etree.HTML(self.response.text)

    @property
    def json(self):
        return json.loads(self.response.text)
    
    @property
    def soup(self):
        return BeautifulSoup(self.response.text, 'html.parser')
    
    @property
    def text(self):
        return self.response.text


class ProxyRequest(Request):
    
    def __init__(
        self, 
        url,
        headers: dict = None, 
        proxies: list = None, 
        timeout: int = None, 
        retry: int = None, 
        retry_delay: float = None,
        verbose: bool = True,
        **kwargs
    ):
        super().__init__(url, headers, **kwargs)
        self.proxies = get_proxy(page_size=20) if proxies is None else proxies
        self.timeout = 2 if timeout is None else timeout
        self.retry = len(self.proxies) if retry is None else retry
        self.retry_delay = 0 if retry_delay is None else retry_delay
        self.kwargs = kwargs
        self.verbose = verbose
    
    def get(self):
        if isinstance(self.proxies, dict):
            self.proxies = [self.proxies]
        random.shuffle(self.proxies) 
        for try_times, proxy in enumerate(self.proxies):
            if try_times + 1 <= self.retry:
                try:
                    response = requests.get(self.url, headers=self.headers, proxies=proxy, timeout=self.timeout, **self.kwargs)
                    response.raise_for_status()
                    self.response = response
                    if self.verbose:
                        print(f'[+] {self.url}, try {try_times + 1}/{self.retry}')
                    return self
                except Exception as e:
                    if self.verbose:
                        print(f'[-] [{e}] {self.url}, try {try_times + 1}/{self.retry}')
                    time.sleep(self.retry_delay)

    def post(self):
        if isinstance(self.proxies, dict):
            self.proxies = [self.proxies]
        random.shuffle(self.proxies) 
        if self.retry == -1:
            self.retry = len(self.proxies)
        for try_times, proxy in enumerate(self.proxies):
            if try_times + 1 <= self.retry:
                try:
                    response = requests.post(self.url, headers=self.headers, proxies=proxy, **self.kwargs)
                    response.raise_for_status()
                    self.response = response
                    if self.verbose:
                        print(f'[+] {self.url}, try {try_times + 1}/{self.retry}')
                    return self
                except Exception as e:
                    if self.verbose:
                        print(f'[-] [{e}] {self.url}, try {try_times + 1}/{self.retry}')
                    time.sleep(self.retry_delay)

    def process(self):
        raise NotImplementedError


@Cache(directory=None, prefix='proxy', expire_time=2592000)
def get_proxy(page_size: int = 20):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"
    }
    url_list = [f'https://free.kuaidaili.com/free/inha/{i}/' for i in range(1, page_size + 1)]
    proxies = []
    for url in url_list:
        data = pd.read_html(url)[0][['IP', 'PORT', '类型']].drop_duplicates()
        print(f'[+] {url} Get Success!')
        data['类型'] = data['类型'].str.lower()
        proxy = (data['类型'] + '://' + data['IP'] + ':' + data['PORT'].astype('str')).to_list()
        proxies += list(map(lambda x: {x.split('://')[0]: x}, proxy))
        time.sleep(0.8)
    available_proxies = []
    
    for proxy in proxies:
        try:
            res = Request('https://www.baidu.com', 
                headers=headers, proxies=proxy, timeout=1).get().response
            res.raise_for_status()
            available_proxies.append(proxy)
        except Exception as e:
            print(str(e))
    
    print(f'[=] Get {len(proxies)} proxies, while {len(available_proxies)} are available. '
        f'Current available rate is {len(available_proxies) / len(proxies) * 100:.2f}%')
    return proxies

@Cache(directory=None, prefix='holidays', expire_time=7776000)
def chinese_holidays():
    root = 'https://api.apihubs.cn/holiday/get'
    complete = False
    page = 1
    holidays = []
    while not complete:
        params = f'?field=date&holiday_recess=1&cn=1&page={page}&size=366'
        url = root + params
        data = Request(url).get().json['data']
        if data['page'] * data['size'] >= data['total']:
            complete = True
        days = pd.DataFrame(data['list']).date.astype('str')\
            .astype('datetime64[ns]').to_list()
        holidays += days
        page += 1
    return holidays

try:
    CHD = chinese_holidays()
    CBD = pd.offsets.CustomBusinessDay(holidays=CHD)
except:
    CBD = pd.offsets.BusinessDay()
    CHD = []
