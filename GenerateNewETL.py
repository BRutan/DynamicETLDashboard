#####################################
# GenerateNewETL.py
#####################################
# Description:
# * Generate report detailing column attributes of
# columns in file, and SQL table definition(s)
# recommended for dataset based upon report.

from argparse import ArgumentParser, ArgumentError
from Columns.DataColumnAttributes import DataColumnAttributes
from ETL.NewETLAppender import NewETLAppender
from ETL.PostArgsFactory import PostArgsFactory
import json
import os
import re
from Utilities.FileConverter import FileConverter
from Utilities.Helpers import ConvertDateFormat, GetRegexPattern, IsRegex
from Utilities.LoadArgs import GenerateNewETLJsonArgs

def GenerateColumnAttributesReport():
    print("------------------------------")
    print("GenerateNewETL")
    print("------------------------------")
    # Get script arguments:
    args = GenerateNewETLJsonArgs()
    ###############################
    # Get all column attributes for data file(s):
    ###############################
    print ("Reading all data files at")
    print (args.datapath)
    attributes = DataColumnAttributes()
    reportpath = "%s%s.xlsx" % (args.outputfolder, args.etlname)
    attributes.GetDataAttributes(args.datapath,args.filedateinfo,args.filenamereg,args.convertedpaths,args.sheets)
    attributes.GenerateReport(reportpath)
    print ("Finished generating report at")
    print (reportpath)
    # Generate sql table definition based upon column attributes:
    tableDefPath = "%s%s.sql" % (args.outputfolder, args.tablename)
    print ("Generating table definition for %s at " % args.tablename)
    print (tableDefPath)
    attributes.CreateTableDefinitions(args.outputfolder, args.tablename)
    print ("Finished generating table definitions.")
    # Generate new service appsettings-template.json (for committing to project) 
    # and Appsettings.json (for testing locally) files based upon new etl:
    updatedTemplatePath = "%sappsettings-template.json" % args.outputfolder
    updatedAppsettingsPath = "%sAppsettings.json" % args.outputfolder
    print ("Appending new ETL configuration to appsettings-template.json file at")
    print (updatedTemplatePath)
    dateConfigStr = "{RegPattern:'(?<date>%s)', DateFormat:'%s'}" % (GetRegexPattern(args.filedateinfo['regex']), ConvertDateFormat(args.filedateinfo['dateformat']))
    preops = [{"TypeName" : "AddFileDate", "ConfigValue": dateConfigStr}]
    kwargs = { 'tablename' : args.tablename, 'preoperations' : preops }
    appender = NewETLAppender(args.etlname, args.appsettingstemplate, args.config, kwargs)
    appender.OutputUpdatedTemplateFile(updatedTemplatePath)
    appender.OutputUpdatedAppsettingsFile(updatedAppsettingsPath)
    # Generate postargs to folder:
    postargsPath = "%spostargs.json" % args.outputfolder
    print ("Generating postargs.json containing DynamicETL.WebAPI post arguments at")
    print (postargsPath)
    # PostArgsFactory.GeneratePostArgs(outpath = postargsPath, etlname = args.etlname, datafilepath = None)

if __name__ == "__main__":
    GenerateColumnAttributesReport()
