#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" mysql 8.0 root 用户启用mysql_native_password
select user,host,plugin from mysql.user;
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '123456';
FLUSH PRIVILEGES;
"""
import pymysql
from core.util.config import global_config
from core.util.passwd_util import decrypt_password


class MYSQL(object):
    def __init__(self):
        self.host = global_config.get_value("mysql", "host")
        self.port = int(global_config.get_value("mysql", "port"))
        self.user = global_config.get_value("mysql", "user")
        self.password = decrypt_password(global_config.get_value("mysql", "password"))

    def get_schema_connect(self, schema):
        return pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=schema,
                               charset="utf8")

    def execute_sql_fetch_all(self, mysql_connect, sql):
        """执行查询语句,获取所有结果 tuple
        """
        mysql_cursor = mysql_connect.cursor()
        mysql_cursor.execute(sql)
        mysql_connect.commit()
        return mysql_cursor.fetchall()

    def execute_sql(self, mysql_connect, sql):
        mysql_cursor = mysql_connect.cursor()
        mysql_cursor.execute(sql)
        mysql_connect.commit()

    def get_table_comments(self, db, tab):
        connection = self.get_schema_connect(db)
        cursor = connection.cursor()
        # 取表注释
        comment_sql = "SELECT t2.TABLE_COMMENT FROM information_schema.TABLES t2 " \
                      "WHERE t2.table_schema = lower('{0}') " \
                      "and t2.table_name = lower('{1}')".format(db, tab)
        cursor.execute(comment_sql)
        table_comment = cursor.fetchone()
        table_comment = '' if len(table_comment) <= 1 else table_comment[0]
        return table_comment

    def truncate_table(self, db, tab):
        connection = self.get_schema_connect(db)
        truncate_table = "truncate table {0}.{1}".format(db, tab)
        self.execute_sql(connection, truncate_table)

    def get_hive_table_desc_from_hive_metadata(self, hive_schema, hive_table):
        connection = self.get_schema_connect("hive")
        cursor = connection.cursor()
        # 查询hive元数据表字段描述信息
        sql = """
            select 
                 d.name as db_name
                ,t.tbl_name
                ,e.param_value as tbl_comment
                ,c.column_name
                ,c.type_name
                ,c.comment
                ,c.integer_idx
            from hive.TBLS t
            join hive.SDS s on s.sd_id = t.sd_id
            join hive.COLUMNS_V2 c on c.cd_id = s.cd_id
            join hive.DBS d on t.db_id = d.db_id
            join hive.TABLE_PARAMS e on t.tbl_id = e.tbl_id and e.param_key='comment'
            where d.name = '{0}'
              and t.tbl_name = '{1}'
            order by t.tbl_name, c.integer_idx
        """.format(hive_schema, hive_table)
        cursor.execute(sql)
        return cursor.fetchall()

    # 根据mysql 表描述生成hive ddl
    def generate_hive_table_ddl(self, mysql_db, mysql_table, hive_db, hive_table, is_partition=False,
                                is_external=False):
        """
        mysql_db = mysql 数据库
        mysql_table = mysql 表名
        hive_db = hive库名
        hive_table = hive表名
        is_partition : 是否建hive分区表, 默认为非分区表
        is_external : 是否建hive外部表, 默认为非外部表
        """
        location = global_config.get_value("hive", "table_location")
        delimited = global_config.get_value("hive", "row_format_delimited")
        mysql_conn = self.get_schema_connect(mysql_db)
        table_comment = self.get_table_comments(mysql_db, mysql_table)
        if is_external:
            create_head = '''create external table if not exists {0}.{1}(\n'''.format(hive_db, hive_table)
        else:
            create_head = '''create table if not exists {0}.{1}(\n'''.format(hive_db, hive_table)
        if is_partition:
            create_tail = "comment '{0}' \n" \
                          "partitioned by(inc_day string comment '分区字段yyyyMMdd') \n" \
                          "row format delimited fields terminated by '{1}' \n" \
                          "location '{2}{3}.db/{4}' ".format(table_comment, delimited, location, hive_db, hive_table)
        else:
            create_tail = "comment '{0}' \n" \
                          "row format delimited fields terminated by '{1}' \n" \
                          "location '{2}{3}.db/{4}'".format(table_comment, delimited, location, hive_db, hive_table)
        try:
            # 获取一个游标
            with mysql_conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                sql = 'SHOW FULL FIELDS FROM {0}'.format(mysql_table)
                cursor.execute(sql)  # 返回记录条数
                try:
                    for row in cursor:  # cursor.fetchall()
                        if 'bigint' in row['Type']:
                            row['Type'] = "bigint"
                        elif 'binary' in row['Type'] or 'varbinary' in row['Type']:
                            row['Type'] = "binary"
                        elif 'json' in row['Type']:
                            row['Type'] = "map<string,string>"
                        elif 'int' in row['Type'] or 'tinyint' in row['Type'] or 'smallint' in row[
                            'Type'] or 'mediumint' in row['Type'] or 'integer' in row['Type']:
                            row['Type'] = "int"
                        elif 'double' in row['Type'] or 'float' in row['Type'] or 'decimal' in row['Type']:
                            row['Type'] = "double"
                        else:
                            row['Type'] = "string"
                        create_head += row['Field'].lower() + ' ' + row['Type'] + ' comment \'' + row[
                            'Comment'] + '\' ,\n'
                except Exception as ex:
                    raise Exception("Generate hive table ddl exception", ex)
        finally:
            mysql_conn.close()
        hive_ddl_str = create_head[:-2] + '\n' + ') ' + create_tail
        return hive_ddl_str  # 返回hive建表语句


if __name__ == '__main__':
    mysql = MYSQL()
    result = mysql.generate_hive_table_ddl("pasepd", "dim_date", "ods", "dim_date", True, True)
    print(result)
