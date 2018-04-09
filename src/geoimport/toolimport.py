# coding=UTF-8

''' 导入工具基类'''
class toolimport:
    m_Config=None
    def __init__(self,toolconfig):
        self.m_Config=toolconfig

    def runimport(self):
        if self.m_Config is None:
            print "无法运行导入操作，配置信息为空"
            return False