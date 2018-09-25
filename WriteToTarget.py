#!/usr/bin/env python
# coding: utf-8

# In[15]:


from pyspark.sql import SparkSession
from TargetInfo import TargetInfo as targetInfo

class WriteToTarget():
    
    def __init__():
        print("Initialize write Target Class")
        
    #input spark type(SparkSession)
    @staticmethod
    def writeToHive(spark, targetDF):
        print("Pushing final data to Target")
        target=targetInfo.targetMetaData["dbName"]+"."+targetInfo.targetMetaData["tgtTableName"]
        #Push Data to Hive
        selectCol=""
        delm="("
        for col, colType in targetDF.dtypes:
            selectCol=selectCol+delm+col+" "+colType
            delm=","

        selectCol=selectCol+")"
        targetDF.show()
        createStmnt="create table IF NOT EXISTS "+target+selectCol+" stored as "+targetInfo.targetMetaData["tableFormat"]
        spark.sql(createStmnt)
        targetDF.createOrReplaceTempView("tempDF")
        
        if(targetInfo.targetMetaData["writeMode"].lower()=="append"):
            spark.sql("insert into "+target+" select * from tempDF")
        elif(targetInfo.targetMetaData["writeMode"].lower()=="overwrite"):
            spark.sql("insert overwrite table "+target+" select * from tempDF")
        else:
            raise Exception('InCorrect Insert Mothod')
        





