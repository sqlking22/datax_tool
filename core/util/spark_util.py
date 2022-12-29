#!/usr/bin/python3
# -*- coding: utf-8 -*-
import re
from pyspark.sql import SparkSession
from core.util.mysql_util import MYSQL


class PYSPARK(object):
    def __init__(self):
        self.spark_conn = SparkSession.builder.master("yarn").appName('spark_etl') \
            .config("spark.sql.crossJoin.enabled", "true") \
            .config("spark.num.executors", "2") \
            .config("spark.executor.cores", "1") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memoryOverhead", "2048") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.port.maxRetries", "1000") \
            .enableHiveSupport() \
            .getOrCreate()

    def execute_spark_sql(self, sql):
        print(sql)
        try:
            self.spark_conn.sql(sql).show()
        except Exception as e:
            print("spark execute_sql sql Error:" + sql)
            print(e)
            return False
        return True

    def get_hive_count(self, database, table):
        sql = "select count(1) cnt from %s.%s" % (database, table)
        print(sql)
        try:
            df = self.spark_conn.sql(sql)
        except Exception as e:
            print("spark get_count sql Error:" + sql)
            print(e)
            return False, None
        return True, df.collect()[0].cnt

    def get_hive_columns(self, sql):
        sql = str(sql).replace("`", "")
        print(sql)
        try:
            df = self.spark_conn.sql(sql)
        except Exception as e:
            print("spark get_columns sql Error:" + sql)
            print(e)
            return False, None
        return True, df.columns

    def clean_dataframe_dirty_data(self, tmp_dataframe):
        col_name_str = ''
        """df.dtypes
        [('age', 'int'), ('name', 'string')]
        """
        dtypes = tmp_dataframe.dtypes
        for col_type in dtypes:
            if col_type[1] == "string":
                col_name_str += ' regexp_replace(' + col_type[
                    0] + ',"\\\\\\\\n|\\\\\\\\t|\\\\\\\\r|\\\\n|\\\\t|\\\\r"," ")as ' + col_type[0] + ""","""
            else:
                col_name_str += col_type[0].strip() + ""","""
        col_name = col_name_str[:-1]
        return col_name

    def if_exists_hive_table(self, db_name, table_name):
        self.spark_conn.sql("use %s" % db_name)
        # ===>  database tableName isTemporary
        table_names = self.spark_conn.sql("show tables").rdd.map(lambda p: p[1]).collect()
        if table_name in table_names:
            return True
        else:
            return False

    def load_data_from_db(self, host, port, user, password, url, db, tb, driver):
        df = None
        try:
            if 'select' in tb:
                db_table = """(""" + tb + """) t"""
            else:
                db_table = tb
            df = self.spark_conn.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", db_table) \
                .option("user", user) \
                .option("password", password) \
                .option("fetchsize", 10000) \
                .option("driver", driver) \
                .load()
        except Exception as e:
            print("spark get dataframe Error:" + tb, e)
        finally:
            return df

    def write_df_to_db(self, dataframe, user, password, db, tab, url, driver, mode="append"):
        dataframe.write \
            .mode(mode) \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", "%s.%s" % (db, tab)) \
            .option("user", user) \
            .option("password", password) \
            .option("batchsize", 2000) \
            .option("driver", driver) \
            .save()

    def to_mysql_type(self, hive_type):
        if re.match(r'bigint|int|smallint|tinyint', hive_type, flags=0):
            return 'bigint'
        elif re.match(r'varbinary|binary', hive_type, flags=0):
            return 'binary'
        elif re.match(r'double|float|decimal', hive_type, flags=0):
            return 'double'
        elif re.match(r'map<string,string>', hive_type, flags=0):
            return 'json'
        elif re.match(r'timestamp|date', hive_type, flags=0):
            return 'datetime'
        else:
            return 'VARCHAR(800)'

    # 根据hive元数据生成mysql建表语句
    def get_mysql_ddl_from_hive_metadata(self, hive_schema, hive_table, mysql_db, mysql_table):
        mysql = MYSQL()
        result = mysql.get_hive_table_desc_from_hive_metadata(hive_schema, hive_table)
        count = len(result)
        if count > 0:
            create_head = "create table if not exists {0}.{1} (".format(mysql_db, mysql_table)
            cou = 1
            create_mid = ""
            for rs in result:
                column = str(rs[3]).lower()
                clo_type = self.to_mysql_type(rs[4])
                clo_comment = "COMMENT '{}' ".format(str(rs[5]).replace('\r\n|\r|\n', ''))
                if cou == 1:
                    create_mid += column + " " + clo_type + " " + clo_comment + "\n"
                else:
                    create_mid += ',' + column + " " + clo_type + " " + clo_comment + "\n"
                cou += 1
            create_table_sql_end = ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{0}' ".format(result[0][2])
            create_table_sql = create_head + create_mid + create_table_sql_end
            return create_table_sql


if __name__ == '__main__':
    spark = PYSPARK()
    hive_schema = ''
    hive_table = ''
    mysql_db = ''
    mysql_table = ''
    spark.get_mysql_ddl_from_hive_metadata(hive_schema, hive_table, mysql_db, mysql_table)
