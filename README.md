# sQuery

[![Java Build](https://github.com/ousheobin/sQuery/actions/workflows/maven.yml/badge.svg)](https://github.com/ousheobin/sQuery/actions/workflows/maven.yml)

一个很好玩的支持单表十亿行的百分比查询小工具 （百分比以外功能正在添加 ing)

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
$ git clone https://github.com/ousheobin/sQuery.git
$ cd sQuer
$ mvn clean package -DskipTests
```

进入 target， 可以找到开头为 squery 的压缩包。
