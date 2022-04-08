# 因子测试框架

## 因子定义

因子定义可以结合Wind数据库完成，Wind数据库中保存有计算因子相关必需的数据，可以调用编写好的数据获取函数实现因子的计算。

本地计算完成的因子可以通过pandas读取的方式直接输入框架，但是数据必需满足如下的格式：

1. 因子名称必需作为列，一列代表一个因子
2. 因子数据的索引必需是双索引，第一维度索引名为name，第二维度索引名为asset；在第一个纬度上是所有的因子数据的日期，，第二维度上是个股/行业的名称
3. 因子数据如果要进行Barra回归计算，必需要携带一列名称为group的列（也正是这个原因，因子名称不能定义为group），表示各个asset所在的行业
4. 要进行回归，IC或是分层的计算，必须要保证数据带有未来收益，因此含有未来收益，因子值以及group行业的数据被称为标准数据

## 因子测试

因子测试的流程较为简单，主要是使用analyze中的三大函数进行测试，测试时除了Barra回归必需携带行业数据外，其他函数都不是必需携带行业数据的。使用如下：

```python
import dropbear.utils as dbu

# get data
standarized_data = dbu.prepare.factor_datas_and_forward_returns('return_1m', ['2022-01-04', '2022-01-05'], forward_period=20)
# analyze them
reg_result = dbu.analyze.regression(standarized_data)
ic_result = dbu.analyze.ic(standarized_data)
layer_result = dbu.analyze.layering(standarized_data)
# visulization
dbu.visualize.regression_plot(reg_result)
dbu.visulize.ic_plot(ic_result)
dbu.visulize.layering_plot(layer_result)
```

## TODO

- 加入机器学习算法至analyze中，可以使用机器学习的回归与分类方法进行计算
