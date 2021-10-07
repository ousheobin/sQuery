# sQuery

[![Java Build](https://github.com/ousheobin/sQuery/actions/workflows/maven.yml/badge.svg)](https://github.com/ousheobin/sQuery/actions/workflows/maven.yml)

一个很好玩的支持单表十亿行的百分比查询小工具。

sQuery 的想法和核心代码来自于之前参与的一个分析引擎设计活动，需要设计一个多线程的引擎支持对 10 亿行多列的数据表进行准确的百分比查询。

目前只支持查询 long 类型的数据，double 类型正在添加。

（百分比以外各种功能正在添加 ing...)

## 环境要求

- Java 1.8 或以上版本
- 建议使用 Linux 或 MacOS 系统

## 怎么玩？🚀

从 Release 下载咱们的 zip 包，或从代码构建（参见下面一小节）

解压 Zip 包，得到一个叫 `squery` 的目录，使用命令行：

```shell
# 进入目录 :)
cd squery

# 加载数据文件（支持 csv 格式）
./sQuery load /data/data.csv

# 加载数据文件, 并指定名字为 table
./sQuery load /data/data.csv -name table

# 加载文件夹中的所有数据文件
./sQuery load /data/

# 查询 sell 表中，sell_cnt 字段的中位数
./sQuery median sell sell_cnt

# 查询 sell 表中，sell_cnt 字段 top 91% 的准确数据值
./sQuery quantile sell sell_cnt 91.0000%

# 删除 data 表
./sQuery delete data
```

工具在 Linux / MacOS 中会默认使用 `/tmp/squery/workspace` 作为临时工作空间，请保证目录可写

## 本地怎么打包？

首先，你需要有 JDK 1.8 和 Maven 3.3.9 （及以上版本）

```shell
git clone https://github.com/ousheobin/sQuery.git
cd sQuery
mvn clean package -DskipTests
```

进入 target 目录， 可以找到开头为 squery 的压缩包。

## 核心的想法

1. load 阶段会根据表、列进行数据存储，并统计分布信息，以便高效查询
2. 使用数值的前 n 位二进制值进行分桶，每次查询只需要载入少量数据即可
3. 使用 Quick Select 算法，不需要排序即可找到 Top K 的值

## 致谢

本项目的场景与测试数据来自于阿里云天池竞赛平台的样例。([传送门](https://tianchi.aliyun.com/competition/entrance/531895/information))