#!/usr/bin/env python
# coding: utf-8

# In[ ]:

from os import listdir, path
from json import loads
import sys

# imports downloaded from PyPi
from pyspark import SparkFiles
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *

# local dependencies
from dependencies import logging

from SourceInfo import SourceInfo as sourceInfo
from TargetInfo import TargetInfo as targetInfo
from ReadProperty import ReadProperty as readProperty
from ReadSource import ReadSource as readSource
from RuleTransformation import RuleTransformation as ruleTransformation

class SparkMain():
    
    sourceDF=None
    
    def main():
        """Main Spark script definition.
        :return: None
        """
        applicatioName=sys.argv[1]
        sparkSess=SparkSession.builder.appName(applicatioName).getOrCreate()
        logger=logging.Log4j(sparkSess)
        sourceDF=None
        logger.warn("DP_INFO: DP_INFO: Application Started and SparkSession Created")
        
        logger.warn("DP_INFO: Calling parameter read method")
        readProperty.allotValue(sparkSess, applicatioName)
        logger.warn("DP_INFO: Parsed parameter file succesfully")
        
        logger.warn("DP_INFO: Extract data from source and create DataFrame")
        sourceDF=readSource.extract_data(sparkSess,logger)
        logger.warn("DP_INFO: Extracted data from source "+sourceInfo.sourceData["sourceType"]+", and created  sourceDataFrame.")
        
        logger.warn("DP_INFO: Starting Data Validation and Data Quality Check Operations on SourceDF")
        sourceDF=ruleTransformation.process_rule(sparkSess, sourceDF)
        
    
    if __name__ == '__main__':
        main()
