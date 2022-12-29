#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64


def encrypt_password(password):
    bytes_passwd = password.encode("utf-8")
    str_passwd = base64.b64encode(bytes_passwd)
    return str_passwd


def decrypt_password(bytes_passwd=None):
    bytes_passwd = "WmFHVm1rWVhWc2RBPT0s" if bytes_passwd is None else bytes_passwd
    try:
        password = str(base64.b64decode(bytes_passwd).decode("utf-8"))
    except Exception as e:
        password = ""
        print("获取密码出错。" + e)
        raise Exception()
    return password


if __name__ == '__main__':
    password = encrypt_password("123456")
    print(password)




