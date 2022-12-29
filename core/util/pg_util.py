#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import psycopg2
from core.util.config import global_config
from core.util.passwd_util import decrypt_password


class POSTGRES(object):
    def __init__(self):
        self.host = global_config.get_value("postgres", "host")
        self.port = int(global_config.get_value("postgres", "port"))
        self.user = global_config.get_value("postgres", "user")
        self.password = decrypt_password(global_config.get_value("postgres", "password"))

    # 获取pg库连接
    def get_pg_connect(self):
        try:
            pg_connect = psycopg2.connect(user=self.user, password=self.password, host=self.host, port=self.port)
            return pg_connect
        except (psycopg2.Error, Exception) as err:
            raise Exception("could not connect to pg server", err)

    def get_pg_schema_connect(self, schema):
        try:
            pg_connect = psycopg2.connect(database=schema, user=self.user, password=self.password, host=self.host,
                                          port=self.port)
            return pg_connect
        except (psycopg2.Error, Exception) as err:
            raise Exception("could not connect to pg server", err)

    def get_table_comments(self, table_name):
        pg_conn = self.get_pg_connect()
        sql = "select cast(obj_description(relfilenode,'pg_class') as varchar) as comment " \
              "from pg_class c where relname ='%s'" % table_name
        try:
            cur = pg_conn.cursor()
            cur.execute(sql)
            table_comments = cur.fetchone()
            table_comment = '' if len(table_comments) <= 1 else table_comments[0]
            return table_comment
        except psycopg2.Error as ex:
            print(ex)

    def get_column_comments(self, table_name):
        pg_conn = self.get_pg_connect()
        sql = """
        select a.attnum,a.attname,concat_ws('',t.typname,SUBSTRING(format_type(a.atttypid,a.atttypmod) from '.∗')) 
        as type,d.description from pg_class c, pg_attribute a , pg_type t, pg_description d where c.relname = '%s' 
        and a.attnum>0 and a.attrelid = c.oid and a.atttypid = t.oid and d.objoid=a.attrelid and d.objsubid=a.attnum
        """ % table_name
        try:
            cur = pg_conn.cursor()
            cur.execute(sql)
            column_comments = cur.fetchall()
            return column_comments
        except psycopg2.Error as ex:
            print(ex)

    # 判断是否存在pg表
    def if_exists_table(self, table_name):
        pg_conn = self.get_pg_connect()
        exists = False
        try:
            cur = pg_conn.cursor()
            cur.execute("select exists(select relname from pg_class where relname='" + table_name + "')")
            exists = cur.fetchone()[0]
            cur.close()
        except psycopg2.Error as e:
            print(e)
        return exists

    def execute_sql(self, sql):
        pg_conn = self.get_pg_connect()
        cursor = pg_conn.cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)
            return False
        pg_conn.commit()
        return True

    def get_pg_metadata(self, table_name):
        pg_conn = self.get_pg_connect()
        sql = """select * from (
        select 
        (nc.nspname)::information_schema.sql_identifier AS table_schema, 
        (c.relname)::information_schema.sql_identifier AS table_name, 
        (a.attname)::information_schema.sql_identifier AS column_name, 
        (a.attnum)::information_schema.cardinal_number AS ordinal_position,
        (t.typname)::information_schema.character_data AS data_type,
        (information_schema._pg_char_max_length(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)))::information_schema.cardinal_number AS character_maximum_length,
        (information_schema._pg_numeric_precision(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)))::information_schema.cardinal_number AS numeric_precision, 
        (information_schema._pg_numeric_scale(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)))::information_schema.cardinal_number AS numeric_scale,
        (d.description) as column_description,
        (c.oid) as oid,
        (de.description) as table_description
        FROM (    
              (select * from pg_attribute where attnum > 0 AND (NOT attisdropped))a 
              JOIN 
              (select oid,relowner,relname,relnamespace from pg_class where relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'f'::"char"])) c ON a.attrelid = c.oid
              JOIN 
              (select nspname,oid from pg_namespace  where (NOT pg_is_other_temp_schema(oid)))nc  ON c.relnamespace = nc.oid
              JOIN 
              pg_type t ON a.atttypid = t.oid
              left join
              pg_description d on d.objoid = c.oid and d.objsubid=a.attnum	
              left join
              (select objoid,description from pg_description where objsubid=0) de on de.objoid = c.oid
              left join 
              (select conrelid,conkey from pg_constraint where contype='p') con on con.conrelid = c.oid and a.attnum = any(con.conkey)
        ) 
        WHERE 
            (pg_has_role(c.relowner, 'USAGE'::text) 
            OR has_column_privilege(c.oid, a.attnum, 'SELECT, INSERT, UPDATE, REFERENCES'::text))
        ) t where table_name = '{0}'
        order by t.oid,t.ordinal_position
        """.format(table_name)
        cursor = pg_conn.cursor()
        cursor.execute(sql)
        result_rows = cursor.fetchall()
        pg_conn.close()
        return result_rows

    # 根据pg元数据生成hive建表语句
    def generate_hive_table_ddl(self, pg_db, pg_table, hive_db, hive_table, is_partition=False, is_external=False):
        """
        pg_db = pg 数据库
        pg_table = pg 表名
        hive_db = hive库名
        hive_table = hive表名
        is_partition : 是否建hive分区表, 默认为非分区表
        is_external : 是否建hive外部表, 默认为非外部表
        """
        table_comment = self.get_table_comments(pg_table)
        metadata = self.get_pg_metadata(pg_table)
        location = global_config.get_value("hive", "table_location")
        delimited = global_config.get_value("hive", "row_format_delimited")
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
            for row_tuple in metadata:
                row_list = list(row_tuple)
                if 'int8' in row_list[4]:
                    row_list[4] = "bigint"
                elif 'int1' in row_list[4] or 'int2' in row_list[4] or 'int4' in row_list[4]:
                    row_list[4] = "int"
                elif 'double' in row_list[4] or 'float4' in row_list[4] or 'float8' in row_list[4]:
                    row_list[4] = "double"
                elif 'numeric' in row_list[4]:
                    row_list[4] = "DECIMAL(" + '38' + "," + '10' + ")"
                elif 'datetime' in row_list[4] or 'timestamp' in row_list[4] or 'timestamptz' in row_list[4]:
                    row_list[4] = "timestamp"
                else:
                    row_list[4] = "string"
                row_list[8] = '' if row_list[8] is None else row_list[8]
                create_head += row_list[2].lower() + ' ' + row_list[4] + ' comment \'' + row_list[8] + '\' ,\n'
        except Exception as ex:
            raise Exception("Generate hive table ddl exception", ex)
        create_str = create_head[:-2] + '\n' + ')' + create_tail
        return create_str  # 返回字段列表与建表语句


if __name__ == '__main__':
    pg = POSTGRES()
    result = pg.generate_hive_table_ddl("pasepd", "dim_date", "ods", "dim_date", True, True)
    print(result)
