#####################################
# EvaluateData.py
#####################################
# Description:
# * Generate report detailing column information
# in one or more files intended to be stored. Generate
# table definition for appropriate SQL language.


from argparse import ArgumentParser, ArgumentError
from Columns.DataColumnAttributes import DataColumnAttributes
from ETL.NewETLAppender import NewETLAppender
from ETL.PostArgsFactory import PostArgsFactory
import json
import os
import re
from Utilities.FileConverter import FileConverter
from Utilities.Helpers import ConvertDateFormat, GetRegexPattern, IsRegex
from Utilities.LoadArgs import EvaluateDataJsonArgs

def EvaluateData():
    """
    * Perform key steps in order.
    """
    print ("------------------------------")
    print ("EvaluateData")
    print ("------------------------------")
    # Get script arguments:
    args = EvaluateDataJsonArgs()
    attributes = GenerateReportAndTable(args)
    AppendETL(args, attributes)
    GeneratePostArgs(args, attributes)

def GenerateReportAndTable(args):
    """
    * Get all column attributes for data file(s):
    """
    print ("Reading all data files at")
    print (args['data']['path'])
    reportpath = "%s%s_Attributes.xlsx" % (args['outputfolder'],args['processname'])
    attributes = DataColumnAttributes()
    # (path, fileExp, dateFormat = None, filePaths = None, sheets = None, delim = None, recursive = False, rowstart)
    attributes.GetDataAttributes(args['data']['path'],args['filenamereg'],args['filedatereg'],None,args['data']['sheets'],args['data']['delim'],args['recursive'],args['data']['rowstart'])
    attributes.GenerateReport(reportpath)
    print ("Finished generating report at")
    print (reportpath)
    # Generate sql table definition based upon column attributes:
    tableDefPath = "%s%s.sql" % (args.outputfolder, args.tablename)
    print ("Generating table definition for %s at " % args.tablename)
    print (tableDefPath)
    attributes.CreateTableDefinitions(args.outputfolder, args.tablename, args.allnull)
    print ("Finished generating table definitions.")
    return attributes

if __name__ == '__main__':
    EvaluateData()