# MR-DBSCAN

## 简介
参考论文《MR-DBSCAN: a scalable MapReduce-based DBSCAN algorithm for heavily skewed data》
基于Hadoop和Hive实现的大数据密度聚类应用。

## 环境
- Centos-7.6
- JDK-1.8
- Hadoop-2.10.2
- ZooKeeper-3.8.4
- mysql-8.0.26

## 启动
hadoop jar DBSCAN-1.0-MR.jar com.java.MR_DBSCAN
[范围] [核心邻居数] [本地数据集] [纬度] [最大分区数据数] [输出地址]

## 数据形式
按照 [id x1 x2 ... xn] 格式在数据文件中排布

## 输出
聚类后的二维图，仅考虑前两个纬度