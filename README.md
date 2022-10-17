
# 简易结构化流程开发

**为什么建立这个repository**

这里是谦豫曾经完成的一些简易的结构化项目，每一个项目的代码都是交付后保留下来的，可以正常运行。
在完成上述项目的同时，本人总结了一套数据挖掘工作的开发流程，供读者参考（随着完成更多的项目，本人对开发的理解更加深入，会不断加入新的总结）

**数据流构建的三种方式**

- 方式一：在大数据开发平台(dataworks)上，通过探索原始数据+梳理业务逻辑，在平台反复尝试，写出正确的SQL代码，得到结果表
    - 参考项目：nlp_structure_housing_found
- 方式二：将正确的SQL代码封装上shell的外壳，通过调用beeline接口建表，spark-sql接口执行SQL计算逻辑和写数，实现组合SQL的一键运行
    - 参考项目：nlp_structure_insur_model/shell一键化跑数/2.重疾险理赔情况统计/
- 方式三：纯SQL无法实现复杂逻辑，比如，DPCNN分类模型，区别于函数式编程思维，需要面向对象的编程思维才能实现), 通过spark-submit去提交python的脚本和对应的依赖
    - 参考项目：nlp_structure_insur_model/shell一键化跑数/1.保险公司名称标准化/
    - 核心脚本：data_extractor.sh 
        - spark-submit去提交并执行python的脚本和对应的依赖
    - 核心脚本：data_extractor.py 
        - 实现python的计算逻辑
        - spark.sql(_sql)从hive表中读取数据，返回dataframe对象，python开发的用户可以自定义一些复杂计算逻辑，去操作dataframe的数据结构
        - [Spark SQL基本概念和基本用法](https://www.cnblogs.com/swordfall/p/9006088.html)
    - 核心脚本：spark_base_connector.py
        - 对spark_sql的封装，实现了一些常用的功能函数（读表/写表）
- 方式四：将正确的SQL代码使用python脚本衔接起来，通过调用beeline接口建表，spark-sql接口执行SQL计算逻辑和写数，实现组合SQL的一键运行
    - 参考项目：nlp_structure_enterprise_data_statistics
    - 核心脚本：dataflow.py
    - 参考脚本：example_code.py