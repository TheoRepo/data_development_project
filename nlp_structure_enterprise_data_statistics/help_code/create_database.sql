-- 在这里，IF NOT EXISTS是一个可选子句，通知用户已经存在相同名称的数据库。可以使用SCHEMA 在DATABASE的这个命令。下面的查询执行创建一个名为userdb数据库：
CREATE DATABASE IF NOT EXISTS dws_ent;
CREATE SCHEMA dws_ent;