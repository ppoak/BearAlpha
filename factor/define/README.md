# 因子定义

每一个python脚本都是对某一个大类因子的定义，将不同的细分因子在对应大类因子中实现

## 因子描述

| 因子名称 | 因子大类 | 因子定义 |
|:---:|:---:|:---:|
| return_1m | Technical | Stock total return over past 20 trading days |
| return_3m | Technical | Stock total return over past 60 trading days |
| return_12m | Technical | Stock total return over past 250 trading days |
| turnover_1m | Technical | Stock average turnover in past 20 days, MA(VOLUME/CAPITAL, 20) |
| turnover_3m | Technical | Stock average turnover in past 60 days, MA(VOLUME/CAPITAL, 60) |

## 注意事项

因子定义主要分为四个阶段：

1. 获取沪深300股票池以及全市场股票对应的行业信息（本项目采用中信一级行业分类）
2. 获取最少量的数据并计算对应的因子值
3. 对计算出来的结果进行数据预处理，包括去极值、标准化和缺失值填充
4. 将数据格式进行调整，包括修改Series的名称对数据索引进行调整

在第一个阶段，对每一个因子几乎都是相同的操作

在第二个阶段要注意的的问题是一定要保持数据量的最小化，如计算20个交易日的收益率，一定要采用当天与过去20天的当日收盘价，否则获取过多的冗余数据会导致操作时间的大幅度延长

第三个阶段对计算出来的结果进行数据预处理在每一个因子中都是相同的

在最后一个阶段，对数据进行调整，包括将数据调整为Series格式，保持Series的name属性与函数名的一致性，同时将所以调整为双索引，一级索引为日期，用date命名，二级索引为股票代码，用code命名
