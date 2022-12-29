#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import configparser
from date_util import get_current_date


def get_project_name():
    project_name = "datax_tool"
    return project_name


def get_project_path():
    """
    获取当前项目根路径
    :return: 根路径
    """
    project_name = get_project_name()
    cur_path = os.path.abspath(os.path.dirname(__file__))
    root_path = cur_path[:cur_path.find(project_name + os.sep) + len(project_name + os.sep)]
    return root_path


def get_config_path():
    file_path = os.path.abspath(get_project_path() + "conf" + os.sep + "config.ini")
    return file_path


def get_log_path():
    file_path = os.path.abspath(get_project_path() + "logs" + os.sep + get_current_date())
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    return file_path


def get_logfile_abspath():
    log_file_abspath = get_log_path() + os.sep + get_project_name() + ".log"
    return log_file_abspath


def get_json_path():
    json_path = os.path.abspath(get_project_path() + "jsonfile" + os.sep + get_current_date())
    if not os.path.exists(json_path):
        os.mkdir(json_path)
    return json_path


class Config(object):
    def __init__(self, filepath=None):
        if filepath is None:
            filepath = get_config_path()
        else:
            filepath = filepath
        if not os.path.exists(filepath):
            raise FileNotFoundError("No such file: config.ini")
        self.conf = configparser.ConfigParser()
        self.conf.read(filepath, encoding="utf-8")

    def get_value(self, section, key):
        return self.conf.get(section, key)

    def get(self, section, name, strip_blank=True, strip_quote=True):
        s = self.conf.get(section, name)
        if strip_blank:
            s = s.strip()
        if strip_quote:
            s = s.strip('"').strip("'")
        return s

    def getboolean(self, section, name):
        return self.conf.getboolean(section, name)


global_config = Config()

if __name__ == "__main__":
    # read = Config()
    # value = read.get_value("project", "project_name")
    # print(value)
    path = get_json_path()
    print(path)
