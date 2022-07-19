# Quool

## Installation

```
git clone https://github.com/ppoak/quool
git submodule update quool
```

## Secret content

*You have to have access to secret key, local database*. For the sake of data api, address or token should be stored and keep unpublished, quool can deal with these problems by **cache**. You can set, delete or display any key in the cache database. 

### Usage

```shell
python -m quool [-h] {set, delete, show, clear} ...

Quool CLI API

positional arguments:
  {set,delete,show,clear}
    set                 Set cache via quool Cli
    delete              Delete cache via quool Cli
    show                Show cahce keys
    clear               Clear expired keys

optional arguments:
  -h, --help            show this help message and exit
```

### Examples

```shell
# set the key to value
python -m quool set -k testkey -v testvalue
```

```shell
# delete the key
python -m quool delete -k testkey
```

```shell
# show the keys
python -m quool show
```

```shell
# clear the expired keys
python -m quool clear
```

## RiceQuant / LocalDatabase

Data from RiceQuant, any help please contact `ppoak@foxmail.com`

### Initialization

First of all, you should let quool know where your database is. The way of doing this is to set the path or address in the quool cache.

```shell
# using sqlite
python -m pandasquant set -k local -v sqlite:////absolute/path/to/your/database
# using mysql
python -m pandasquant set -k local -v mysql+pymysql://127.0.0.1:3306/stock
```

⚠️*note that the key must be `local` because it is marked in other quantum repositories*

### API

Before using any functions, initialize the database instance first

```python
database = ql.Stock(ql.Cache().get('local'))
```

#### Get market information in daily frequency

```python
database.market_daily(start, end, code, fields, and_, or_)
```

start: str, start date, default to 20100101
end: str, end date, default to today
code: str, code, default to all stocks
fields: str or list, fields you want to get, depend on the database you have, default to all fields
and_: str, 'and' conditions, form like 'adjclose >= 70.00', default to None
or_: str, 'or' conditions, form like 'adjclose >= 70.00', default to None

return: if you set code to a str, returns the normal dataframe or series, if you set a fields to str, returns a series, otherwise returns a multiindex dataframe.

#### Get plate information

```python
database.plate_info(start, end, code, fields, and_, or_)
```

start: str, start date, default to 20100101
end: str, end date, default to today
code: str, code, default to all stocks
fields: str or list, fields you want to get, depend on the database you have, default to all fields
and_: str, 'and' conditions, form like 'source = "citics"', default to None
or_: str, 'or' conditions, form like 'source = "citics"', default to None

return: if you set code to a str, returns the normal dataframe or series, if you set a fields to str, returns a series, otherwise returns a multiindex dataframe.

#### Get index weight

```python
database.index_weights(start, end, code, fields, and_, or_)
```

start: str, start date, default to 20100101
end: str, end date, default to today
code: str, code, default to all stocks
fields: str or list, fields you want to get, depend on the database you have, default to all fields
and_: str, 'and' conditions, default to None
or_: str, 'or' conditions, default to None

return: if you set code to a str, returns the normal dataframe or series, if you set a fields to str, returns a series, otherwise returns a multiindex dataframe.

#### Get index market information in daily frequency

```python
database.market_daily(start, end, code, fields, and_, or_)
```

start: str, start date, default to 20100101
end: str, end date, default to today
code: str, code, default to all stocks
fields: str or list, fields you want to get, depend on the database you have, default to all fields
and_: str, 'and' conditions, default to None
or_: str, 'or' conditions, default to None

return: if you set code to a str, returns the normal dataframe or series, if you set a fields to str, returns a series, otherwise returns a multiindex dataframe.

#### Get instrument information on the market

```python
database.instruments(code, fields, and_, or_)
```

code: str, code, default to all stocks
fields: str or list, fields you want to get, depend on the database you have, default to all fields
and_: str, 'and' conditions, default to None
or_: str, 'or' conditions, default to None

return: a dataframe

### Development

If you get a different database, and you want to rewrite a database interface, here is the reference:

1. Inherit the DataBase class in ql.staff.provider. It is not exposed in the quool package, so you may need to access it on with the package path.
2. Write the interface, basic parameter is `start, end, code, fields, and_, or_`. And what you need to do in the function is call the `get` function in the DataBase class. Except the parameters the function accepts, you should provide some extra information including **date column in the database, code column in the database, table name to fetch the data and index column in the return data**

### Tushare

#### Initialization

You can set tushare token in quool cache like: 

```python 
python -m pandasquant set -k tushare -v xxxxxxxxxx
```

And initialize the tushare api class

```python
tushare = pq.TuShare(pq.Cache().get('token'))
```

### Crawler

At present, quool provide the crawler interface for eastmoney and stockus, which satisfy our need. If you want some undeveloped interfaces, please contact us. But do read the documents of [akshare](https://www.akshare.xyz) or [baostock](https://baostock.com) to see whether they can help you.

### StockUS

#### Index market information in daily frequency

```python
ql.StockUS.index_price(index, start, end)
```

index: str, index code
start: str, start time
end: str, end time

return: a dataframe

#### Market daily information in daily frequency

```python
ql.StockUS.cn_price(code, start, end)
```

code: str, stock code
start: str, start time
end: str, end time

return: a dataframe

#### Report list recently

```python
ql.StockUS.report_list(category, period, q, org_name, author, xcf_years, search_fields, page, page_size)
```

category: str, category of the report
period: str, the recent period to include the report
q: str, key word in report
org_name: str, orgnization name
author: str, author name
xcf_years: str, new fortune year
search_fields: str, the field where the key word appear
page: int, number of page
page_size: int, number of records in each page

return: a dataframe

#### Report search
```python 
ql.StockUS.report_search(period, q, org_name, author_name, xcf_years, search_fields, page, page_size)
```

period: str, the recent period to include the report
q: str, key word in report
org_name: str, orgnization name
author: str, author name
xcf_years: str, new fortune year
search_fields: str, the field where the key word appear
page: int, number of page
page_size: int, number of records in each page

return: a dataframe

### Eastmoney

#### Active oprate department

```python
ql.Em.active_opdep(date)
```

date: str, date of search

return: a dataframe

#### Active oprate department in detail

```python
ql.Em.active_opdep_details(date)
```

date: str, date of search

return: a dataframe

#### Trading institution information

```python
ql.Em.institution_trade(date)
```

date: str, date of search

return: a dataframe

#### Oversea institution position query

```python
ql.Em.oversea_institution_holding(date)
```

date: str, date of search

return: a dataframe

#### Stock buy back information

```python
ql.Em.stock_buyback(date)
```

date: str, date of search

return: a dataframe
