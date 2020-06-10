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
import json
import os
import re
from Utilities.FileConverter import FileConverter
from Utilities.Helpers import IsRegex
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
    # Generate new service appsettings file based upon new etl:
    updatedAppSettingsPath = "%sappsettings-template.json" % args.outputfolder
    print ("Appending new ETL configuration to appsettings-template.json file at")
    print (updatedAppSettingsPath)
    kwargs = { 'tablename' : args.tablename }
    appender = NewETLAppender(args.etlname, args.appsettingstemplate, kwargs)
    appender.OutputUpdatedFile(updatedAppSettingsPath)

if __name__ == "__main__":
    GenerateColumnAttributesReport()
