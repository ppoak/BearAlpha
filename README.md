# Financial Research

![tensorflow](https://img.shields.io/badge/TensorFlow-FF6F00?style=for-the-badge&logo=tensorflow&logoColor=white) ![python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)

![overview](https://activity-graph.herokuapp.com/graph?username=ppoak&theme=minimal)

## 简介

这是与量化研究相关的项目仓库，其中每个量化、金融工程相关的研究都在项目中用文件夹单独列出，里面README文件中包含了相关项目的介绍、文献（文献链接一般都可获取），并对这些研报做复现、提升。

other文件夹中是一些杂乱的想法，还没有能够完全实现或是暂时没有好办法对效果进行提升的指标，这些指标优于散乱或是正处于开发阶段暂时没有使用独立的项目文件夹存放。

utils文件夹中存放的是一些与本地建立的数据库获取数据的函数，详细内容可以参考一下项目`ppoak/database`。

dropbear是量化分析的综合库，包含了本地数据库获取数据并计算因子的相关函数（`dropbear/define`）如有新因子想要定义，可以参考define中的base.py和define中的README说明。`dropbear/core`是分析库的核心，主要负责传入计算好的标准化因子数据并进行各个方面的分析，详细内容可以参考[README](./dropbear/README.md)。

## 项目维护

目前项目正在不断添加新的想法和可以参考的研报内容，如果有不同的想法或是希望获取项目库中相关数据的可以联系邮箱`oakery@qq.com`，也欢迎发起PullRequest，任何问题可以Issue中提出。
