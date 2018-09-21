#!/usr/bin/env python
# coding: utf-8

# In[ ]:


class SourceInfo():
    
    sourceData={"connType":"",
                "hostName":"",
                "port":"",
                "userName":"",
                "password":"",
                "dbName":"",
                "tableName":"",
                "cntlFilePath":"",
                "sourceFilePath":"",
                "masterColumn":"",
                "isFixed":"",
                "columnLength":"",
                "RuleColumn":"",
                "RuleList":"",
                "sourcePrimKey":"",
                "delmiter":"",
                "schemaCheck":"",
    }
    
    def __init__():
        print("Initialize SourceInfo")
    
    @staticmethod
    def set_value(passedKey,Value):
       sourceData[passedKey]=value

