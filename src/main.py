# coding=utf-8
from config import config, importdatatype, TOOLOPERATYPE
from GeoTool import OperatorFactory
from GeoCommon import DbOperator



class TooFactory:
    '''工具工厂'''
    m_Config=None
    def __init__(self,toolConfig):
        self.m_Config=toolConfig

    def RunTool(self):
        if self.m_Config.OPERATETYPE== TOOLOPERATYPE.CREATE:
            createTool=OperatorFactory(self.m_Config)
            createTool.m_ToolOperator.CreatFormData()
        elif self.m_Config.OPERATETYPE==TOOLOPERATYPE.CREATRANDINSERT:
            tool=OperatorFactory(self.m_Config)
            tool.m_ToolOperator.CreateAndInsert()
        elif self.m_Config.OPERATETYPE==TOOLOPERATYPE.INSERT:
            tool=OperatorFactory(self.m_Config)
            tool.m_ToolOperator.InsertData()
        elif self.m_Config.OPERATETYPE==TOOLOPERATYPE.UPDATE:
            tool=OperatorFactory(self.m_Config)
            tool.m_ToolOperator.UpdateData()




if __name__ == "__main__":
    geotool=TooFactory(config)
    geotool.RunTool()
    print "操作成功"
