#!/usr/bin/env python
# coding: utf-8

# In[15]:


from pyspark.sql import SparkSession

class WriteToTarget():
    
    def __init__():
        print("Initialize write Target Class")
        
    #input spark type(SparkSession)
    @staticmethod
    def writeToHive(spark, targetInfo, targetDF):
        print("Pushing final data to Target")
        target=targetInfo.dbName+"."+targetInfo.tgtTableName
        #Push Data to Hive
        selectCol=""
        delm="("
        for col, colType in targetDF.dtypes:
            selectCol=selectCol+delm+col+" "+colType
            delm=","

        selectCol=selectCol+")"
        spark.sql("create table IF NOT EXISTS "+target+selectCol+" row format delemited fields terminated by ',' stored as "+targetInfo.tableFormat)
        spark.createOrReplaceTempView("tempDF")
        if(targetInfo.writeMode.lower()=="append"):
            spark.sql("insert into "+target+" select * from tempDF")
        if(targetInfo.writeMode.lower()=="overWrite"):
            spark.sql("insert overwrite table "+target+" select * from tempDF")
        else:
            raise Exception('InCorrect Insert Mothod')
        





