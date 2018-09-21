#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from SourceInfo import SourceInfo as sourceInfo
from TargetInfo import TargetInfo as targetInfo
from pyspark import SparkFiles

class ReadProperty():
    
    @staticmethod
    def allotValue(sparkSess, applicatioName):
        propFile=sparkSess.read.format("csv").option("delimiter","=").load(SparkFiles.get(applicatioName+".properties")).rdd.map(list)
        for key,value in propFile:
            if value!=None:
                if (key in sourceInfo.sourceData.keys()):
                    sourceInfo.sourceData[key]=value
                if (key in targetInfo.sourceData.key()):
                    targetInfo.targetMetaData[key]=value
        

