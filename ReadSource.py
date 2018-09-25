#!/usr/bin/env python
# coding: utf-8

# In[18]:
from pyspark.sql import SparkSession
import logging
from SourceInfo import SourceInfo as sourceInfo

class ReadSource():
    
    def __init__():
        print("Initialize Read Source Class")
    
    @staticmethod
    def extract_data(spark,logger):
        if(sourceInfo.sourceData["connType"].lower()=="hive"):
            return ReadSource.readFromHive(spark,logger)
        if((sourceInfo.sourceData["connType"].lower()=="hdfs") or (sourceInfo.sourceData["connType"].lower()=="localfs")):
            return ReadSource.readFromFS(spark,logger)
        if(sourceInfo.sourceData["connType"].lower()=="mysql"):
            return ReadSource.readFromMySQL(spark)
            
    @staticmethod    
    def readFromFS(spark,logger):
        logMessage="DP_INFO: Extracting data from HDFS"
        hdfsPath=sourceInfo.sourceData["path"]
        if(sourceInfo.sourceData["connType"]=="LocalFS"):
            hdfsPath="/user/app/"+sourceInfo.sourceData["dpName"]+"/*"
            logMessage="DP_INFO: Extracting data from LocalFS"
        logger.warn(logMessage)
        tempDF=spark.read.format("csv").option("delimeter",sourceInfo.sourceData["delimeter"]).option("header",'true').schema(sourceInfo.sourceData["sourceSchema"]).path(hdfsPath)
        return tempDF
    @staticmethod
    def readFromMySQL(spark,logger):
        logger.warn("DP_INFO: Extracting data from MySQL")
        jdbc_url="jdbc:mysql://{0}:{1}/{2}".format(sourceInfo.sourceData["host"], sourceInfo.sourceData["port"], sourceInfo.sourceData["dbName"])
        tempDF= spark.read.format("jdbc").options(url=jdbc_url,driver="com.mysql.jdbc.Driver",dbtable =sourceInfo.sourceData["tableName"],user=sourceInfo.sourceData["userID"],password=sourceInfo.sourceData["password"]).load()
        return tempDF
    @staticmethod
    def readFromHive(spark,logger):
        logger.warn("DP_INFO: Extracting data from Hive")
        source=sourceInfo.sourceData["dbName"]+"."+sourceInfo.sourceData["tableName"]
        tempDF=spark.table(source)
        return tempDF
        


# In[ ]:





# In[ ]:




