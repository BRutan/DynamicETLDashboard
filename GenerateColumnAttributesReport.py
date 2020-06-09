#####################################
# GenerateColumnAttributesReport.py
#####################################
# Description:
# * Generate report detailing column attributes of
# columns in file, and SQL table definition(s)
# recommended for dataset based upon report.

from argparse import ArgumentParser, ArgumentError
from Columns.DataColumnAttributes import DataColumnAttributes
import json
import os
import re
from Utilities.FileConverter import FileConverter
from Utilities.Helpers import IsRegex
from Utilities.LoadArgs import GenerateColumnAttributesReportJsonArgs

def GenerateColumnAttributesReport():
    print("------------------------------")
    print("GenerateColumnAttributesReport")
    print("------------------------------")
    # Get script arguments:
    args = GenerateColumnAttributesReportJsonArgs()
    ###############################
    # Get all column attributes for data file(s):
    ###############################
    print ("Reading all data files at")
    print (args.datapath)
    attributes = DataColumnAttributes()
    attributes.GetDataAttributes(args.datapath,args.filedateinfo,args.filenamereg,args.convertedpaths,args.sheets)
    attributes.GenerateReport(args.reportpath)
    print ("Finished generating report at")
    print (args.reportpath)
    tableDefOutput = args.reportpath[0:args.reportpath.rfind('\\')] + '\\'
    print ("Generating table definition for %s at " % args.tablename)
    print (tableDefOutput)
    attributes.CreateTableDefinitions(tableDefOutput, args.tablename)
    print ("Finished generating table definitions.")


if __name__ == "__main__":
    GenerateColumnAttributesReport()
