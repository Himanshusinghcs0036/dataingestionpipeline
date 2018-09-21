#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 14:54:05 2018

@author: xeadmin
"""
from pyspark import SparkFiles
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *

# local dependencies
from dependencies import logging
from SourceInfo import SourceInfo as sourceInfo

class RuleTransformation():
    
    @staticmethod
    def process_rule(spark, inputDF, logger):
        rule_list=sourceInfo.sourceData['RuleList']
        rule_col_list=sourceInfo.sourceData['RuleColumn']
        sourceInfo.sourceData['sourcePrimKey']
        sourceInfo.sourceData['delmiter']
        
        for rule_name, rule_column, supporting_field in rule_list.split(","),rule_col_list.split("&"), supporting_data.split("&"):
            
            logger.warn("DP_INFO: Starting Rule Implementation for rule "+rule_name+" for rule columns  "+rule_column);
            if(rule_name.lower()=="nullcheck"):
                #callNullCheck
                inputDF=RuleTransformation.null_check(inputDF, rule_column)
            if(rule_name.lower()=="dedup"):
                #callDedup
            if(rule_name.lower()=="validvalue"):
                #callvalidvalues
            if(rule_name.lower()=="invalidvalues"):
                #InvalidValues
            if(rule_name.lower()=="integercheck"):
                #callInteger
            if(rule_name.lower()=="numericcheck"):
                #callNNumericheck
            if(rule_name.lower()=="rangecheck"):
                #callRange
            if(rule_name.lower()=="lengthcheck"):
                #calllengthChecker
            if(rule_name.lower()=="cdc"):
                #perfromCDC
            
    @staticmethod
    def null_check(inputDF, ruleColumn):
        filterCond=""
        delm=""
        for col in ruleColumns.split(","):
            filterList=filterList+delm+ col+ " is not null"
            delm=" and "
        
        outputDF=inputDF.filter(filterCond)
        
    @staticmethod
    def dedup(inputDF, ruleColumn):
        dropColList=[]
        for col in ruleColumns.split(","):
            dropColList.append(col.strip())
        
        outputDF=inputDF.dropDuplicates(dropColList)
        
    @staticmethod
    def isValid(inputDF, ruleColumn, validValues, rule_name):
        validFlag=True
        if(rule_name.lower()=="invalidvalues"):
            validFlag=False
            
        validValueList=[]
        for value in validValues.split(","):
            validValueList.append(value)
            
        outputDF=inputDF.filter(inputDF[ruleColumn].isin(validValueList) == vallidFlag)
       
    @staticmethod  
    def isNumeric(inputDF, ruleColumn, rule_name):
        regex_pattern="^[0-9.]*$"
        if(rule_name.lower()=="integercheck"):
            regex_pattern="^[0-9]*$"
        
        isStarting=True 
        filterCondition=""
        for col in ruleColumn:
            if(isStarting):
                filterCondition=inputDF[col].rlike(regex_pattern)
                isStarting=False
            else:
                filterCondition=filterCondition & inputDF[col].rlike(regex_pattern)
                
        outputDF=inputDF.filter(filterCondition)
    
    @staticmethod
    def isInRange(inputDF, ruleColumn, rangeValue):
        
        rangeStart=rangeValue.split(",")[0]
        rangeEnd=rangeValue.split(",")[1]
        
        filterCondition=inputDF[col].between(rangeStart,rangeEnd)
        outputDF=inputDF.filter(filterCondition)
        
    @staticmethod    
    def field_length(inputDF, ruleColumn, col_size):
        
       outputDF= inputDF.filter(lit(length(sourceDF1[ruleColumn]))==col_size)
        
#sourceDF=spark.read.format("csv").option("delimeter",",").option("header","true").option("inferSchema","true").load("file:///home/xeadmin/Downloads/SampleData/busin*")
# from pyspark.sql.functions import col, unix_timestamp, to_date
# sourceDF1=sourceDF.withColumn("application_date", to_date(unix_timestamp(sourceDF["application_date"],"mm/dd/yyyy").cast("timestamp")))
# sourceDF1.printSchema()

#
##
#
#      