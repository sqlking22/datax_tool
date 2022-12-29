#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ****************************************************************#
# @Time    : 2022/11/13 14:48
# @Author  : JonHe 
# Function : 使用datax批量从达梦同步所有表数据到mysql
# ****************************************************************#
import re
import subprocess
import sys
import os
import json
import pathlib


class DM2MYSQL:
    def __init__(self):
        read_config = ReadConfig()
        # dm 配置
        self.dm_host = read_config.get_dm("host")
        self.dm_port = int(read_config.get_dm("port"))
        self.dm_user = read_config.get_dm("user")
        self.dm_password = read_config.get_dm("password")

        # mysql 配置
        self.mysql_host = read_config.get_mysql("host")
        self.mysql_port = int(read_config.get_mysql("port"))
        self.mysql_user = read_config.get_mysql("user")
        self.mysql_password = read_config.get_mysql("password")

        self.current_Path = os.path.dirname(os.path.abspath(__file__))
        self.datax_home = os.environ.get('DATAX_HOME', "/home/hadoop/software/datax")
        if self.datax_home is None:
            raise Exception("请先配置datax环境变量")
        self.datax_cmd = "python " + self.datax_home + os.sep + "bin" + os.sep + "datax.py --loglevel warn "
        self.current_day = datetime.datetime.now().strftime("%Y%m%d")
        self.json_path = self.datax_home + os.sep + "job" + os.sep + self.current_day
        if not os.path.exists(self.json_path):
            os.makedirs(self.json_path)
        # 默认在mysql pasepd库，新建datax数据同步日志记录表
        self.datax_log_db = "pasepd"
        self.datax_log_table = "datax_sync_log"


    def if_exists_mysql_table(self, con, table_name):
        cur = con.cursor()
        cur.execute("show tables")
        tables = [cur.fetchall()]
        table_list = re.findall('(\'.*?\')', str(tables))
        table_list = [re.sub("'", '', each) for each in table_list]
        if table_name in table_list:
            return 1  # 存在返回1
        else:
            return 0  # 不存在返回0

    def get_dm_metadata(self, conn, dm_schema, dm_table):
        sql = """
        select  distinct
            A.COLUMN_ID as id,
            A.column_name as Field,
            case when A.data_type in('VARCHAR','CHAR') then concat(A.data_type,'(',A.data_length,')') 
                 when A.data_type in('DEC','DECIMAL') then concat(A.data_type,'(',A.DATA_PRECISION,',',A.DATA_SCALE,')') 
                 else A.data_type
            end as Type,
            B.comments as column_comments,
            C.COMMENTS as table_comments
        from ALL_TAB_COLUMNS A
        left join ALL_COL_COMMENTS B on A.COLUMN_NAME=B.column_name 
              and A.Table_Name =B.Table_Name and A.OWNER =B.OWNER
        left join ALL_TAB_COMMENTS C on A.Table_Name=C.TABLE_NAME and A.OWNER =C.OWNER
        where 1=1
          and A.OWNER='{0}'
          and A.Table_Name='{1}'
        ORDER BY id
        """.format(dm_schema, dm_table)
        cursor = conn.cursor()
        cursor.execute(sql)
        result_rows = cursor.fetchall()
        conn.close()
        return result_rows

    def get_dm_columns(self, db_name, table_name):
        dm_column = []
        dm_conn = self.get_dm_connect(db_name)
        dm_column_meta = self.get_dm_metadata(dm_conn, db_name, table_name)
        for row in dm_column_meta:
            # Field 信息
            dm_column.append(row[1])
        dm_conn.close()
        return dm_column

    def get_mysql_columns(self, mysql_conn, db_name, table_name):
        mysql_column = []
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute("SHOW FULL FIELDS FROM %s.%s" % (db_name, table_name))
        for col in mysql_cursor.fetchall():
            mysql_column.append(col[0])
        mysql_conn.close()
        return mysql_column

    def get_mysql_record_cnt(self, db_name, table_name):
        mysql_conn = self.get_mysql_connect(db_name)
        cursor = mysql_conn.cursor()
        cursor.execute("select count(1) from %s.%s" % (db_name, table_name))
        fetchone = cursor.fetchone()
        mysql_conn.close()
        return fetchone[0]

    def get_dm_record_cnt(self, db_name, table_name):
        dm_conn = self.get_dm_connect(db_name)
        cursor = dm_conn.cursor()
        cursor.execute("select count(1) from %s.%s" % (db_name, table_name))
        fetchone = cursor.fetchone()
        dm_conn.close()
        return fetchone[0]

    # 获取mysql建表语句
    def create_mysql_ddl_sql(self, dm_schema, dm_table, mysql_database, mysql_table):
        dm_connect = self.get_dm_connect(dm_schema)
        metadata = self.get_dm_metadata(dm_connect, dm_schema, dm_table)
        table_desc = '' if metadata[0][4] is None else metadata[0][4]
        create_head = '''create table if not exists {0}.{1}('''.format(mysql_database, mysql_table)
        create_tail = ' ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT=\'{0}\';'.format(table_desc)
        try:
            for row_tuple in metadata:
                row_list = list(row_tuple)
                # dm 与 mysql 字段 mapping
                if 'BYTE' in row_list[2]:
                    row_list[2] = "TINYINT"
                elif 'PLS_INTEGER' in row_list[2]:
                    row_list[2] = "INTEGER"
                elif 'NUMBER' in row_list[2]:
                    row_list[2] = "DECIMAL(" + '38' + "," + '10' + ")"
                elif 'CHARACTER' in row_list[2]:
                    row_list[2] = "CHAR"
                elif 'TIMESTAMP' in row_list[2]:
                    row_list[2] = "DATETIME"
                elif 'LONGVARCHAR' in row_list[2]:
                    row_list[2] = "MEDIUMTEXT"
                elif 'CLOB' in row_list[2]:
                    row_list[2] = "TEXT"
                elif 'IMAGE' in row_list[2] or 'LONGVARBINARY' in row_list[2]:
                    row_list[2] = "BINARY"
                else:
                    row_list[2] = row_list[2]
                row_list[3] = '' if row_list[3] is None else row_list[3]
                create_head += '`' + row_list[1] + '`' + ' ' + row_list[2] + ' comment \'' + row_list[3] + '\' ,\n'
        except Exception as exp:
            print('生成mysql DDL程序异常!', exp)
        create_mysql_ddl = create_head[:-2] + '\n' + ')' + create_tail
        return create_mysql_ddl

    def generate_json(self, source_database, source_table, target_dbname, target_table, mysql_column, dm_column):
        job = {
            "job": {
                "setting": {
                    "speed": {
                        "record": 2048,
                        "byte": 1000,
                        "chanel": 1
                    },
                    "errorLimit": {
                        "record": 0,
                        "percentage": 0.02
                    }
                },
                "content": [{
                    "reader": {
                        "name": "rdbmsreader",
                        "parameter": {
                            "username": self.dm_user,
                            "password": self.dm_password,
                            "column": dm_column,
                            "splitPk": "",
                            "fetchSize": 2048,
                            "where": "1=1",
                            "connection": [{
                                "table": [source_table],
                                "jdbcUrl": [
                                    "jdbc:dm://" + self.dm_host + ":" + str(
                                        self.dm_port) + "/?schema=" + source_database + "&keyWords=COMMENT,STATUS"]
                            }]
                        }
                    },
                    "writer": {
                        "name": "mysqlwriter",
                        "parameter": {
                            "writeMode": "insert",
                            "username": self.mysql_user,
                            "password": self.mysql_password,
                            "column": mysql_column,
                            "session": [
                                "set session sql_mode='ANSI'"
                            ],
                            "preSql": [
                                "delete from @table"
                            ],
                            "connection": [
                                {
                                    "jdbcUrl": "jdbc:mysql://" + self.mysql_host + ":" + str(
                                        self.mysql_port) + "/" + target_dbname + "?autoReconnect=true&useSSL=false&rewriteBatchedStatements=true",
                                    "table":
                                        [target_table]
                                }
                            ]
                        }
                    }
                }]
            }
        }
        with open(os.path.join(self.json_path, ".".join([source_database, source_table, "json"])), "w") as f:
            json.dump(job, f)

    def get_finish_sync_tables_from_db(self, mysql_conn):
        # 查询今天已经同步OK的表
        query_sql = "select concat(lower(src_dbname),'.',lower(src_table))as tab_name from %s.%s " \
                    "where sync_date=current_date() and sync_status = 0 " % (self.datax_log_db, self.datax_log_table)
        cursor = mysql_conn.cursor()
        cursor.execute(query_sql)
        result_rows = cursor.fetchall()
        table_list = []
        for table_name in result_rows:
            table_list.append(table_name[0].lower())
        mysql_conn.close()
        return table_list

    def create_datax_sync_log_table(self, mysql_conn):
        ddl_sql = """
        CREATE TABLE if not exists {0}.{1} (
              `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
              `sync_date` date DEFAULT NULL COMMENT '数据同步日期',
              `src_user` varchar(64) DEFAULT NULL COMMENT '源端数据库用户名',
              `src_ip` varchar(64) DEFAULT NULL COMMENT '源端数据库ip',
              `src_dbtype` varchar(64) DEFAULT NULL COMMENT '源端数据库类型',
              `src_dbname` varchar(64) DEFAULT NULL COMMENT '源端库名',
              `src_table` varchar(256) DEFAULT NULL COMMENT '源端表名',
              `src_table_cnt` bigint DEFAULT NULL COMMENT '源端表记录条数',
              `target_user` varchar(64) DEFAULT NULL COMMENT '目标端数据库用户名',
              `target_ip` varchar(64) DEFAULT NULL COMMENT '目标端数据库ip',
              `target_dbtype` varchar(64) DEFAULT NULL COMMENT '目标端数据库类型',
              `target_dbname` varchar(64) DEFAULT NULL COMMENT '目标端库名',
              `target_table` varchar(256) DEFAULT NULL COMMENT '目标端表名',
              `target_table_cnt` bigint DEFAULT NULL COMMENT '目标端表记录条数',
              `sync_cmd` varchar(1024) DEFAULT NULL COMMENT 'datax同步命令',
              `start_time` timestamp NULL DEFAULT NULL COMMENT '表同步开始时间',
              `end_time` timestamp NULL DEFAULT NULL COMMENT '表同步结束时间',
              `cost_seconds` bigint unsigned NOT NULL DEFAULT '0' COMMENT '执行时间（单位秒）',
              `sync_status` int DEFAULT NULL COMMENT '同步状态：0表示成功,其他表示失败',
              `insert_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '表记录插入时间',
              PRIMARY KEY (`id`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb3 COMMENT='datax数据同步日志记录表';
        """.format(self.datax_log_db, self.datax_log_table)
        cur = mysql_conn.cursor()
        cur.execute(ddl_sql)
        mysql_conn.commit()

    def insert_datax_sync_log_to_db(self, data_tuple):
        mysql_conn = self.get_mysql_connect(self.datax_log_db)
        insert_sql = "insert into {0}.{1} ".format(self.datax_log_db, self.datax_log_table) + \
                     "(sync_date,src_user,src_ip,src_dbtype,src_dbname,src_table,src_table_cnt,target_user,target_ip," \
                     "target_dbtype,target_dbname,target_table,target_table_cnt,sync_cmd,start_time,end_time," \
                     "cost_seconds,sync_status)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cur = mysql_conn.cursor()
        cur.execute(insert_sql, data_tuple)
        mysql_conn.commit()
        mysql_conn.close()

    def start_datax(self, source_db, source_tab):
        print("datax 数据同步开始-------------------------")
        target_dbname = source_db.lower()
        target_table = source_tab.lower()
        print("from：" + source_db + "." + source_tab + "  =====>>  " + "to：" + target_dbname + "." + target_table)
        # 获取mysql连接，查询目标表是否存在
        mysql_conn = self.get_mysql_connect(target_dbname)
        exists_table = self.if_exists_mysql_table(mysql_conn, target_table)
        # 0表示mysql表不存在,执行新建mysql表
        if exists_table == 0:
            print("----- 未查询到目标端mysql表信息 -> {0}，执行新建表操作 -----".format(target_table))
            create_mysql_ddl = self.create_mysql_ddl_sql(source_db, source_tab, target_dbname, target_table)
            print("正在创建目标端mysql表【%s】:\n" % target_table, create_mysql_ddl)
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute(create_mysql_ddl)
            mysql_conn.commit()
            print("目标端mysql表【%s】创建成功！" % target_table)
        # 判断两端字段是否存在差异,有差异，则抛异常结束程序
        dm_columns = self.get_dm_columns(source_db, source_tab)
        mysql_columns = self.get_mysql_columns(mysql_conn, target_dbname, target_table)
        diff_columns = [col for col in dm_columns if col not in mysql_columns]
        if len(diff_columns) > 0:
            raise Exception("源端与目标端表字段存在差异:dm_columns[" + str(diff_columns) + "] not in mysql_columns!")
        print("数据列信息如下：(共计{0}列)\n".format(len(dm_columns)), dm_columns)
        # 生成datax json文件
        # 处理特殊字段名，dm需要在字段间加""，mysql需要在字段间加``
        dm_columns = ["\"" + col + "\"" for col in dm_columns]
        mysql_columns = ["`" + col + "`" for col in mysql_columns]
        self.generate_json(source_db, source_tab, target_dbname, target_table, mysql_columns, dm_columns)
        print("json 生成完成！")

        # 运行抽取脚本
        print("---------------------------------调用抽数脚本--------------------------------")
        json_file = os.path.join(self.json_path, ".".join([source_db, source_tab, "json"]))
        sync_cmd = self.datax_cmd + json_file
        print(sync_cmd)
        start_time3 = datetime.datetime.now()
        dm_cnt = self.get_mysql_record_cnt(source_db, source_tab)
        sync_status = subprocess.call(sync_cmd, shell=True)
        if sync_status:
            print("datax抽数流程失败,报错状态码：", sync_status)
        else:
            print("datax抽数成功！")
        end_time3 = datetime.datetime.now()
        mysql_cnt = self.get_mysql_record_cnt(target_dbname, target_table)
        cost3_seconds = int((end_time3 - start_time3).seconds)
        log_data_tuple = (
            self.current_day, self.dm_user, self.dm_host, "dm", source_db, source_tab, dm_cnt, self.mysql_user,
            self.mysql_host, "mysql", target_dbname, target_table, mysql_cnt, sync_cmd, start_time3, end_time3,
            cost3_seconds, sync_status)
        self.insert_datax_sync_log_to_db(log_data_tuple)
        # 同步表后,清空json文件
        print('脚本执行结束,正在对文件:[' + json_file + ']进行清理...')
        if os.path.exists(json_file):
            os.remove(json_file)
        print("---------------------------------完成抽数流程--------------------------------")

    def diff_table_record_quantity(self):
        # 查询今天已经同步OK，但两边数量不一致的表名称
        mysql_conn = self.get_mysql_connect(self.datax_log_db)
        query_sql = """select concat(lower(src_dbname),'.',lower(src_table))as tab_name from %s.%s 
        where sync_date=current_date() and sync_status = 0 and src_table_cnt <> target_table_cnt
        """ % (self.datax_log_db, self.datax_log_table)
        cursor = mysql_conn.cursor()
        cursor.execute(query_sql)
        result_rows = cursor.fetchall()
        table_list = []
        for table_name in result_rows:
            table_list.append(table_name[0].lower())
        mysql_conn.close()
        return table_list

    def main(self, src_db, src_table):
        try:
            print("正在同步表:%s.%s" % (src_db, src_table))
            start_time2 = datetime.datetime.now()
            self.start_datax(src_db, src_table)
            end_time2 = datetime.datetime.now()
            print("---------------------------------同步" + src_db + "." + src_table + " 表用时总时长:" + str(
                (end_time2 - start_time2).seconds) + " seconds-------------------")
            # 查询同步后两端表记录条数是否一致，不一致则抛出异常
            diff_table = self.diff_table_record_quantity()
            if len(diff_table) > 0:
                raise Exception("两端表记录条数不一致,请人工再次核查:\n %s" % (str(diff_table)))
        except Exception as e:
            print("程序运行异常！！！", e)


if __name__ == '__main__':
    try:
        source_dbname = sys.argv[1]
        source_table = sys.argv[2]
        dm2mysql = DM2MYSQL()
        dm2mysql.main(source_dbname, source_table)
    except Exception as e:
        print("程序运行异常！！！", e)

