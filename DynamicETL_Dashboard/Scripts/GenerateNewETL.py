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
    """
    * Perform key steps in order.
    """
    print ("------------------------------")
    print ("GenerateNewETL")
    print ("------------------------------")
    # Get script arguments:
    args = GenerateNewETLJsonArgs()
    attributes = GenerateReportAndTable(args)
    AppendETL(args, attributes)
    GeneratePostArgs(args, attributes)

def GenerateReportAndTable(args):
    """
    * Get all column attributes for data file(s):
    """
    print ("Reading all data files at")
    print (args.datapath)
    attributes = DataColumnAttributes()
    reportpath = "%s%s.xlsx" % (args.outputfolder, args.etlname)
    attributes.GetDataAttributes(args.datapath,args.filedateinfo,args.filenamereg,args.convertedpaths,args.sheets,args.delim)
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
    
def AppendETL(args, attributes):
    """
    * Generate new DynamicETL.Service appsettings-template.json (for committing to project) 
    Appsettings.json (for testing locally), and FileWatcher AppSettings-template.json files 
    based upon new etl:
    """
    updatedAppsettingsPath = "%sAppsettings.json" % args.outputfolder
    updatedTemplatePath = "%sappsettings-template.json" % args.outputfolder
    print ("Appending new ETL configuration to Appsettings.json and appsettings-template.json files at")
    print (updatedAppsettingsPath)
    print (updatedTemplatePath)
    dateConfigStr = "{RegPattern:'(?<date>%s)', DateFormat:'%s'}" % (GetRegexPattern(args.filedateinfo['regex']), ConvertDateFormat(args.filedateinfo['dateformat']))
    preops = [{"TypeName" : "AddFileDate", "ConfigValue": dateConfigStr}]
    kwargs = { 'preoperations' : preops }
    # Append one new ETL per sheet if data file contains multiple sheets:
    if not attributes.Sheets is None:
        appender = None
        for sheet in attributes.Sheets:
            etlname = "%s.%s" % (args.etlname, sheet)
            etlname = etlname.replace(' ', '').replace('-', '')
            kwargs['tablename'] = "%s.%s" % (args.tablename, sheet.replace(' ', '').replace('-', ''))
            if not appender is None:
                appender.AppendNewETL(etlname, kwargs) 
            else:
                appender = NewETLAppender(etlname, args.appsettingstemplate, args.config, kwargs)
    else:
        kwargs['tablename'] = args.tablename
        appender = NewETLAppender(args.etlname, args.appsettingstemplate, args.config, kwargs)
    appender.OutputUpdatedTemplateFile(updatedTemplatePath)
    appender.OutputUpdatedAppsettingsFile(updatedAppsettingsPath)

def GeneratePostArgs(args, attributes):
    """
    * Generate postargs to folder:
    """
    postargsPath = "%spostargs.json" % args.outputfolder
    #print ("Generating postargs.json containing DynamicETL.WebAPI post arguments at")
    #print (postargsPath)
    #PostArgsFactory.GeneratePostArgs(outpath = postargsPath, etlname = args.etlname, datafilepath = attributes.FilePaths[0])

if __name__ == "__main__":
    GenerateColumnAttributesReport()
