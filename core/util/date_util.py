#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import datetime


def get_current_date():
    """获取今日日期
    return: '2017-12-13'
    """
    return time.strftime("%Y-%m-%d")


def get_now():
    """获取今日日期:年月日
    return: '20171213'
    """
    return time.strftime("%Y%m%d")


def get_current_timestamp():
    """获取当前时间戳
    return: 1524032735404
    """
    return int(round(time.time() * 1000))


def get_diff_two_second(second1, second2):
    """获取两个秒参数之间的差异
    return: D days, HH小时MM分钟SS秒
    """
    return millisecond_to_time(second2 - second1)


def get_current_time():
    """获取当前时间 带精度
    :return: 时间类型 2021-06-12 00:27:38.065190
    """
    return datetime.datetime.now()


def get_time():
    """获取当前时间 不带精度
    => 格式: '2017-12-13 16:32:30'
    """
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def week_of_month(date):
    """当前天是当月中的第几周
    :param date:
    :return:第几周
    """
    end = int(date.strftime('%W'))
    start = int(datetime.datetime(date.year, date.month, 1).strftime('%W'))
    return end - start


def format_timestamp_to_date(timestamp):
    """毫秒转日期
    :param timestamp: 1524032735404
    :return: 2017-12-13 16:32:30
    """
    time_local = time.localtime(timestamp / 1000)
    return time.strftime("%Y-%m-%d %H:%M:%S", time_local)


def format_date_to_timestamp(str_date):
    """日期转毫秒
    :param str_date: 2017-12-13 16:32:30
    :return: 1524032735404
    """
    strptime = time.strptime(str_date, "%Y-%m-%d %H:%M:%S")
    mktime = int(time.mktime(strptime) * 1000)
    return mktime


def get_time_before(days=0, hours=0, minutes=0, seconds=0, microseconds=0):
    """获取时间偏移数据
    :param days:
    :param hours:
    :param minutes:
    :param seconds:
    :param microseconds:
    :return: 获取当前时间戳=> 格式: '2017-12-13 16:32:30'
    """
    res = datetime.datetime.now() - datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds,
                                                       microseconds=microseconds)
    return res


def millisecond_to_time(sec, num_sec=0):
    """ Convert seconds to 'D days, HH:MM:SS.FFF'
    :param sec :表示 秒
    :param num_sec: 表示保留精度，3表示保留3位，0表示不保留进度
    :return D days, HH小时MM分钟SS秒
    """
    if hasattr(sec, '__len__'):
        return [millisecond_to_time(s) for s in sec]
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    if num_sec > 0:
        pattern = '%%02d小时%%02d分钟%%0%d.%df秒' % (num_sec + 3, num_sec)
    else:
        pattern = r'%02d小时%02d分钟%02d秒'
    if d == 0:
        return pattern % (h, m, s)
    return ('%d days, ' + pattern) % (d, h, m, s)


if __name__ == '__main__':
    print(get_time_before())
    print(week_of_month(get_time_before()))
    time_stamp = format_date_to_timestamp("2017-12-13 16:32:30")
    print(time_stamp)
    sec_time = millisecond_to_time(1200)
    print(sec_time)
