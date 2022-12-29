#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import dmPython
from core.util.config import global_config
from core.util.passwd_util import decrypt_password


class DM(object):
    def __init__(self):
        self.host = global_config.get_value("dm", "host")
        self.port = int(global_config.get_value("dm", "port"))
        self.user = global_config.get_value("dm", "user")
        self.password = decrypt_password(global_config.get_value("dm", "password"))

    # 获取dm库连接
    def get_dm_connect(self, dbname):
        try:
            dm_conn = dmPython.connect(user=self.user, password=self.password, server=self.host, port=self.port)
            dm_cursor = dm_conn.cursor()
            dm_cursor.execute("set schema %s" % dbname)
            print("dm current_schema：", dm_conn.current_schema)
            return dm_conn
        except (dmPython.Error, Exception) as err:
            print("could not connect to DM8 server", err)

    def get_table_name(self, database):
        connection = self.get_dm_connect(database)
        cursor = connection.cursor()
        sql = "SELECT distinct TABLE_NAME from dba_tables WHERE owner='{0}'".format(database)
        cursor.execute(sql)
        fetchall = cursor.fetchall()
        return fetchall

    def get_column_comments(self, dm_schema, dm_table):
        connection = self.get_dm_connect(dm_schema)
        cursor = connection.cursor()
        sql = """
        select  distinct
            A.COLUMN_ID as id,
            A.column_name as Field,
            case when A.data_type in('VARCHAR','CHAR') then concat(A.data_type,'(',A.data_length,')') 
                 when A.data_type in('DEC','DECIMAL') then concat(A.data_type,'(',A.DATA_PRECISION,',',A.DATA_SCALE,')') 
                 else A.data_type
            end as Type,
            B.comments as column_comments
        from ALL_TAB_COLUMNS A
        left join ALL_COL_COMMENTS B on A.COLUMN_NAME=B.column_name 
              and A.Table_Name =B.Table_Name and A.OWNER =B.OWNER
        where 1=1
          and A.OWNER='{0}'
          and A.Table_Name='{1}'
        ORDER BY id
        """.format(dm_schema, dm_table)
        cursor.execute(sql)
        result_rows = cursor.fetchall()
        return result_rows

    def get_table_comments(self, dm_schema, dm_table):
        connection = self.get_dm_connect(dm_schema)
        cursor = connection.cursor()
        sql = "select distinct C.COMMENTS as table_comments from ALL_TAB_COMMENTS C " \
              "where C.OWNER='{0}' and C.TABLE_NAME = '{1}'".format(dm_schema, dm_table)
        cursor.execute(sql)
        result_rows = cursor.fetchall()
        return result_rows


if __name__ == '__main__':
    dm = DM()
    db = "pasepd"
    name = dm.get_table_name(db)
    print(name)
