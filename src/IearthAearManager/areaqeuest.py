# coding=UTF-8
import urllib, urllib2
import httplib


class areaquest:
    m_LoginUrl = "http://192.168.1.28/thinkphp/Admin/User/do_login"

    def __init__(self, loginUrl):
        self.m_LoginUrl = loginUrl

    def Login(self):
        login_data = {"username": "admin1", "password": "e10adc3949ba59abbe56e057f20f883e"}
        data_urlencode = urllib.urlencode(login_data)
        req = urllib2.Request(url= self.m_LoginUrl, data=data_urlencode)
        print req
        res_data = urllib2.urlopen(req)
        res = res_data.read()
        print res