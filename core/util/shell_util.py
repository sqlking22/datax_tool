#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess
import sys
from config import get_logfile_abspath
from date_util import get_time, get_current_date, get_current_time


class Shell(object):
    def __init__(self):
        self.log_file = get_logfile_abspath()

    def run_background(self, cmd):
        """以非阻塞方式执行shell命令（Popen的默认方式）。
        """
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # 非阻塞
        # 执行结果状态码，0表示执行成功，其他表示执行失败
        result_code = process.wait()
        return result_code

    def start_running(self, cmd):
        start_time = get_time()
        current_date = get_current_date()
        start_timestamp = get_current_time()
        std_out = open(self.log_file, 'a+')
        std_out.write(start_time + ":Running------------------------------------" + "\n")
        std_out.write("--------------------【正在运行】:[" + cmd.strip() + "]" + "\n")
        std_out.write("--------------------【开始执行】:" + start_time + "\n")
        std_out.flush()  # 将日志写入文件
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=std_out, shell=True)
        state_code = p.wait()
        end_time = get_time()
        end_timestamp = get_current_time()
        cost_time = str((end_timestamp - start_timestamp).total_seconds())
        std_out.write("--------------------【结束执行】:" + end_time + "\n")
        std_out.write("--------------------【执行时长】:" + cost_time + "秒" + "\n")
        if state_code == 0:
            std_out.write("--------------------【运行状态】:" + "[执行成功]" + "\n")
        else:
            std_out.write("--------------------【运行状态】:" + "[执行失败]" + "\n")
        std_out.write(end_time + ":End-----------------------------------------" + "\n")
        exec_info = (current_date, cmd, start_time, end_time, cost_time, state_code)
        return exec_info


if __name__ == "__main__":
    shell = Shell()
    cmd = "echo Hello World!"
    return_info = shell.start_running(cmd)
    if return_info[5] == 0:
        print("[" + cmd.strip() + "] 执行成功！")
    else:
        print("[" + cmd.strip() + "] 执行失败,请查看报错日志,路径如下：" + "\n" + shell.log_file)
        sys.exit(return_info[5])
