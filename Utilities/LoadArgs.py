#####################################
# LoadArgs.py
#####################################
# Description:
# * Define all functions that load arguments 
# into runscripts (AddFileWatcherConfig.py, 
# GenerateColumnAttributesReport.py, TestETLPipeline.py, 
# ValidateAndAppendNewETL.py).

import json
import re
import os
import sys
from Utilities.Helpers import StringIsDT

############################
# GenerateColumnAttributesReport.py
############################
class Arguments(object):
    """
    * Validate and store script arguments.
    """
    __ReqArgs = { 'data' : False, 'reportpath' : False, 'filedatereg' : False, 'tablename' : False }
    def __init__(self, args):
        args = { arg.lower() : args[arg] for arg in args }
        Arguments.__CheckArgs(args)
        self.datapath = args['data']['path'].replace('R:\\', '\\\\wanlink.us\\dfsroot\\APPS\\')
        self.reportpath = args['reportpath']
        self.filedateinfo = { key.lower() : args['filedatereg'][key] for key in args['filedatereg'] }
        self.filedateinfo['regex'] = re.compile(self.filedateinfo['regex'])
        self.tablename = args['tablename']
        self.filenamereg = None
        self.sheets = args['data']['sheets'] if 'sheets' in args['data'] else None
        self.convertedpaths = None
        if 'filenamereg' in args:
            self.filenamereg = re.compile(args['filenamereg'])
        if 'convert' in args:
            print("Converting all files at '%s'" % (args['convert']['convertpath']))
            print("to '%s" % args['convert']['toextension'])
            # Convert all files before pulling:
            self.convertedpaths = FileConverter.ConvertAllFilePaths(args['convert']['convertpath'], args['convert']['toextension'], 
                                              filereg = self.filenamereg, folderpath = self.datapath)
            self.datapath = args['convert']['convertpath'].replace('R:\\', '\\\\wanlink.us\\dfsroot\\APPS\\')
    @staticmethod
    def __CheckArgs(args):
        """
        * Check argument validity, throw exception if any are invalid.
        """
        errs = []
        req = Arguments.__ReqArgs.copy()
        for arg in args:
            if arg.lower() in req:
                req[arg.lower()] = True

        missing = [arg for arg in req if not req[arg]]
        if missing:
            raise Exception(' '.join(['The following required arguments are missing:', ', '.join(missing)]))

        # "data" arguments:
        if 'path' not in args['data']:
            missing.append('data::path')
        elif not os.path.isdir(args['data']['path']):
            errs.append('data::path must point to folder.')
        elif not os.path.exists(args['data']['path']):
            errs.append(' '.join(['(data::path)', args['data']['path'], ' does not exist.']))
        if 'sheets' in args['data'] and not isinstance(args['data']['sheets'], list):
            errs.append('data::sheets must be a list.')
        # "filedatereg" arguments:
        if not IsRegex(args['filedatereg']['Regex']):
            errs.append(' '.join(['(filedatereg)', args['filedatereg']['Regex'], 'Not a valid regular expression.']))

        # "reportpath" arguments:
        if '.' not in args['reportpath']:
            errs.append('(reportpath) Must point to a file.')
        elif '\\' in args['reportpath'] and not os.path.exists(args['reportpath'][0:args['reportpath'].rfind('\\')]):
            errs.append('(reportpath) Enclosing folder does not exist.')
        elif os.path.exists(args['reportpath']):
            filename, extension = os.path.splitext(args['reportpath'])
            count = 2
            while os.path.exists(args['reportpath']):
                args['reportpath'] = "%s_%d%s" % (filename, count, extension)
                count += 1

        # "filenamereg" arguments:
        if 'filenamereg' in args and not IsRegex(args['filenamereg']):
            errs.append(' '.join(['(filenamereg) ', args['filenamereg'], ' is not a valid regular expression.']))

        # "convert" arguments:
        if 'convert' in args:
            if not ('convertpath' in args['convert'] and 'toextension' in args['convert']):
                errs.append('convert requires "toextension" and "convertpath" as attributes.')
            elif '.' not in args['convert']['toextension']:
                errs.append('%s is invalid conversion extension.' % args['convert']['toextension'])
            if 'convertpath' in args['convert'] and not os.path.exists(args['convert']['convertpath']):
                errs.append('convertpath does not exist.')

        if missing:
            errs.append('The following required subarguments are missing: {%s}' % ', '.join(missing))
        if errs:
            raise Exception("\n".join(errs))


def GenerateColumnAttributesReportCMDLineArgs():
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

def GenerateColumnAttributesReportJsonArgs():
    """
    * Pull script arguments from local json file.
    """
    path = 'GenerateColumnAttributesReport.json'
    if not os.path.exists(path):
        raise Exception(''.join([path, ' does not exist.']))
    return Arguments(json.load(open(path, 'rb')))

############################
# TestETLPipeline.py
############################
def TestETLPipelineJsonArgs():
    """
    * Pull and validate arguments from local json file.
    """
    req_args = set(['fixedargs', 'testetlargs'])
    req_args_fixed = set(['dynamicetlservicepath','logpath','postargspath','serviceappsettingspath','webapipath','webapiurl'])
    req_args_test = set(['etlname','filedate','localtest','reportpath','sqlconnection'])
    req_postargs = set(['id', 'fileid', 'subject', 'arg', 'fileName'])
    req_postargs_arg = set(['FilePath'])
    args = json.load(open('TestETLPipeline.json', 'rb'))
    
    # Ensure required arguments are present:
    errs = []
    missing = req_args - set(args)
    if missing:
        errs.append('The following required toplevel args are missing: %s' % ','.join(missing))
    if 'fixedargs' in args:
        missing_fixed = req_args_fixed - set(args['fixedargs'])
        if missing_fixed:
            errs.append('The following required fixedargs are missing: %s' % ','.join(missing_fixed))
    if 'testetlargs' in args:
        missing_test = req_args_test - set(args['testetlargs'])
        if missing_test:
            errs.append('The following required testetlargs are missing: %s' % ','.join(missing_test))
    if errs:
        raise Exception('\n'.join(errs))
    ########################
    # fixedargs:
    ########################
    # dynamicetlservicepath:
    if not os.path.exists(args['fixedargs']['dynamicetlservicepath']):
        errs.append('(dynamicetlservicepath) Path does not exist.')
    elif not args['fixedargs']['dynamicetlservicepath'].endswith('.exe'):
        errs.append('(dynamicetlservicepath) Path must point to dll.')

    # logpath:
    if not os.path.isdir(args['fixedargs']['logpath']):
        errs.append('(logpath) Must point to a folder.')
    elif not os.path.exists(args['fixedargs']['logpath']):
        errs.append('(logpath) Folder does not exist.')

    # postargspath:
    if not os.path.exists(args['fixedargs']['postargspath']):
        errs.append('(postargspath) Path does not exist.')
    elif not args['fixedargs']['postargspath'].endswith('.json'):
        errs.append('(postargspath) Path must point to a json file.')
    else:
        post_args = json.load(open(args['fixedargs']['postargspath'], 'rb'))
        missing = req_postargs - set(post_args)
        if missing:
            errs.append('(postargspath) The following required arguments in json file are missing: {%s}' % ','.join(missing))
        else:
            args['fixedargs']['postargs'] = post_args
    # Get sample file name from post args file:
    if 'postargs' in args['fixedargs'] and 'arg' in args['fixedargs']['postargs']:
        match = re.search('[A-Z]:.+', args['fixedargs']['postargs']['arg'])
        if not match:
            errs.append('(postargs) arg::FilePath is not a valid path.')
        else:
            args['testetlargs']['samplefile'] = match[0].strip("{}'")
    if 'samplefile' in args['testetlargs'] and not os.path.exists(args['testetlargs']['samplefile']):
        errs.append('(postargs) File at arg::FilePath does not exist.')

    # serviceappsettingspath:
    if not os.path.exists(args['fixedargs']['serviceappsettingspath']):
        errs.append('(serviceappsettingspath) Does not exist.')
    elif not args['fixedargs']['serviceappsettingspath'].endswith('.json'):
        errs.append('(serviceappsettingspath) Must point to .json file.')
    else:
        try:
            args['fixedargs']['appsettings'] = json.load(open(args['fixedargs']['serviceappsettingspath'], 'rb'))
        except Exception as ex:
            errs.append('(serviceappsettingspath) Appsettings json file has following issue: %s' % str(ex))

    # webapipath:
    if not os.path.exists(args['fixedargs']['webapipath']):
        errs.append('(webapipath) Path does not exist.')
    elif not args['fixedargs']['webapipath'].endswith('.dll'):
        errs.append('(webapipath) Path must point to dll.')

    ########################
    # testetlargs:
    ########################
    # etlname: ensure etl is present in appsettings file:
    if 'appsettings' in args['fixedargs'] and not args['testetlargs']['etlname'] in args['fixedargs']['appsettings']['Etls']:
        errs.append('(etlname) ETL %s not in appsettings file.' % args['testetlargs']['etlname'])
    elif 'appsettings' in args['fixedargs']:
        etl = args['testetlargs']['etlname']
        # Ensure that appsettings json file has all necessary keys:
        if 'TableName' not in args['fixedargs']['appsettings']['Etls'][etl]:
            errs.append('(serviceappsettingspath) Missing "TableName" property key for %s etl.' % etl)
        else:
            args['testetlargs']['tablename'] = args['fixedargs']['appsettings']['Etls'][etl]['TableName']
        if 'Destination' not in args['fixedargs']['appsettings']['Etls'][etl]:
            errs.append('(serviceappsettingspath) Missing "Destination" property key for %s etl.' % etl)
        elif 'Destinations' not in args['fixedargs']['appsettings']:
            errs.append('(serviceappsettingspath) Missing "Destinations" property key in json file.')
        else:
            dbHandle = args['fixedargs']['appsettings']['Etls'][etl]['Destination']
            if dbHandle not in args['fixedargs']['appsettings']['Destinations']:
                errs.append('(serviceappsettingspath) Missing %s handle in "Sources".' % dbHandle)
            elif 'ConfigValue' not in args['fixedargs']['appsettings']['Destinations'][dbHandle]:
                errs.append('(serviceappsettingspath) Missing "ConfigValue" key in "Destinations" key in json file.')
            else:
                config = args['fixedargs']['appsettings']['Destinations'][dbHandle]['ConfigValue']
                source = re.search("Data Source=[aA-zZ]+;", config)
                if not source:
                    errs.append('(serviceappsettingspath) Missing "Data Source" in "Destinations::%s::ConfigValue".' % dbHandle)
                else:
                    dbName = source[0].split('=')[1].strip(';')
                    args['testetlargs']['database'] = dbName
            
    # filedate: 
    if not StringIsDT(args['testetlargs']['filedate'], False):
        errs.append('(filedate) %s is invalid date string.' % args['testetlargs']['filedate'])
    else:
        args['testetlargs']['filedate'] = StringIsDT(args['testetlargs']['filedate'], True)

    # localtest:
    args['testetlargs']['localtest'] = args['testetlargs']['localtest'].lower()
    if not args['testetlargs']['localtest'] in ['true', 'false']:
        errs.append('(localtest) Must be true/false.')
    else:
        args['testetlargs']['localtest'] = args['testetlargs']['localtest'] == 'true'

    # reportpath:
    if not args['testetlargs']['reportpath'].endswith('.xlsx'):
        errs.append('(reportpath) Must point to xlsx file.')

    if errs:
        raise Exception('\n'.join(errs))

    return args