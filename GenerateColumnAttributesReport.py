#####################################
# GenerateColumnAttributesReport.py
#####################################
# Description:
# * Generate report detailing column attributes of
# columns in file to assist in creating SQL table to
# hold said columns.

from argparse import ArgumentParser, ArgumentError
from Columns.DataColumnAttributes import DataColumnAttributes
from Helpers import IsRegex
import json
import os
import re

class Arguments(object):
    """
    * Validate and store script arguments.
    """
    __ReqArgs = { 'datapath' : False, 'reportpath' : False, 'filedatereg' : False, 'tablename' : False }
    def __init__(self, args):
        args = { arg.lower() : args[arg] for arg in args }
        Arguments.__CheckArgs(args)
        self.datapath = args['datapath'].replace('R:\\', '\\\\wanlink.us\\dfsroot\\APPS\\')
        self.reportpath = args['reportpath']
        self.filedateinfo = { key.lower() : args['filedatereg'][key] for key in args['filedatereg'] }
        self.filedateinfo['regex'] = re.compile(self.filedateinfo['regex'])
        self.tablename = args['tablename']
        self.filenamereg = None
        if 'filenamereg' in args:
            self.filenamereg = re.compile(args['filenamereg'])
    
    @staticmethod
    def __CheckArgs(args):
        """
        * Check argument validity, throw exception if any failed.
        """
        errs = []
        req = Arguments.__ReqArgs.copy()
        for arg in args:
            if arg in req:
                req[arg] = True

        missing = [arg for arg in req if not req[arg]]
        if missing:
            raise Exception(' '.join(['The following required arguments are missing:', ','.join(missing)]))

        if '.' in args['datapath']:
            errs.append('datapath must point to folder.')
        elif not os.path.exists(args['datapath']):
            errs.append(' '.join(['(datapath)', args['datapath'], 'does not exist.']))

        if not IsRegex(args['filedatereg']['Regex']):
            errs.append(' '.join(['(filedatereg)', args['filedatereg']['Regex'], 'Not a valid regular expression.']))

        if '.' not in args['reportpath']:
            errs.append('(reportpath) Must point to a file.')
        elif '\\' in args['reportpath'] and not os.path.exists(args['reportpath'][0:args['reportpath'].rfind('\\')]):
            errs.append('(reportpath) Enclosing folder does not exist.')
        elif os.path.exists(args['reportpath']):
            path = args['reportpath']
            args['reportpath'] = "%s_1.%s" % (path[0:path.find('.') - 1], path[path.find('.') + 1:])

        if 'filenamereg' in args and not IsRegex(args['filenamereg']):
            errs.append(' '.join(['(filenamereg)', args['filenamereg'], 'is not a valid regular expression.']))

        if errs:
            raise Exception("\n".join(errs))

def RunReport():
    #args = GetArgsFromCMDLine()
    args = GetArgsFromJson()
    ###############################
    # Get all column attributes for data files:
    ###############################
    print ("Reading all data files at")
    print ("%s" % (args.datapath))
    attributes = DataColumnAttributes()
    attributes.GetDataAttributes(args.datapath, args.filedateinfo, args.filenamereg)
    attributes.GenerateReport(args.reportpath)
    print ("Generating table definition for")
    print (args.tablename)
    attributes.CreateTableDefinition(args.tablename)
    print ("Finished generating report at")
    print (args.reportpath)

def GetArgsFromCMDLine():
    parser = ArgumentParser()
    # Mandatory positional arguments:
    parser.add_argument("datapath", type=str, help="Path to folder containing data.")
    parser.add_argument("filedatereg", type=str, help="Regular expression string for file dates.")
    parser.add_argument("reportpath", type=str, help="Output path for report.")
    # Optional arguments:
    parser.add_argument("--filenamereg", type=str, help="(Optional) Regular expression for files in datapath.")
    # parser.add_argument("--gjsonattr", action="store_true", help="(Optional) Put if want to generate skeleton file for DyETL with column attributes filled in.")

    ###############################
    # Check arg validity:
    ###############################
    args = parser.parse_args()
    errs = []
    args.datapath = args.datapath.replace('R:\\', '\\\\wanlink.us\\dfsroot\\APPS\\')
    if '.' in args.datapath:
        errs.append('datapath must point to folder.')
    elif not os.path.exists(args.datapath):
        errs.append(' '.join(['(datapath)', args.datapath, 'does not exist.']))

    if '.' not in args.reportpath:
        errs.append('reportpath must point to a file.')
    elif os.path.exists(args.reportpath):
        errs.append(' '.join(['(reportpath)', args.reportpath, 'already exists.']))
    if not IsRegex(args.filedatereg):
        errs.append(' '.join(['(filedatereg)', args.filedatereg, 'is not a valid regular expression string.']))

    fileNameReg = None
    if hasattr(args, 'filenamereg') and not IsRegex(args.filenamereg):
        errs.append(' '.join(['(--filenamereg)', args.filenamereg, 'is not a valid regular expression.']))
    elif hasattr(args, 'filenamereg'):
        fileNameReg = args.filenamereg

    if errs:
        raise Exception("\n".join(errs))

    return args

def GetArgsFromJson():
    """
    * Pull script arguments from local json file.
    """
    path = 'GenerateColumnAttributesReport.json'
    if not os.path.exists(path):
        raise Exception(''.join([path, ' does not exist.']))
    return Arguments(json.load(open(path, 'rb')))

if __name__ == "__main__":
    RunReport()
