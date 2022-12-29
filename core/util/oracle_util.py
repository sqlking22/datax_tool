#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 先下载 pip install cx_Oracle
import cx_Oracle
from core.util.config import global_config
from core.util.passwd_util import decrypt_password


class ORACLE(object):
    def __init__(self):
        self.host = global_config.get_value("oracle", "host")
        self.port = int(global_config.get_value("oracle", "port"))
        self.user = global_config.get_value("oracle", "user")
        self.password = decrypt_password(global_config.get_value("oracle", "password"))
        self.db = global_config.get_value("oracle", "database")

    def get_db_connect(self, db):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not db:
            raise (NameError, "没有设置数据库信息")
        self.conn = cx_Oracle.connect(self.user + '/' + self.password + '@' + self.host + '/' + db)
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def get_connect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        self.conn = cx_Oracle.connect(self.user + '/' + self.password + '@' + self.host + '/' + self.db)
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def exec_query(self, sql):
        """执行查询语句
        """
        cur = self.get_connect()
        cur.execute(sql)
        resList = cur.fetchall()
        # 查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def exec_non_query(self, sql):
        """
        执行非查询语句
        """
        cur = self.get_connect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()


def main():
    ms = ORACLE()
    query_result = ms.exec_query("select * from user limit 5")
    print(query_result)


if __name__ == '__main__':
    main()
