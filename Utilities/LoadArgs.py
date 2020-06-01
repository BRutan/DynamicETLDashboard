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
from Utilities.Helpers import FillEnvironmentVariables, IsRegex, LoadJsonFile, StringIsDT

############################
# ETLDashboard
############################
def ETLDashboardJsonArgs():
    """
    * Pull arguments from ETLDashboard.json file.
    """
    errs = []
    req_args_fixed = set(['dynamicetlservicepath','etlfilepaths','filewatcherappsettingstemplatepath','filetransferurl','logpath','serviceappsettingspath','webapipath','webapiurl'])
    if not os.path.exists('ETLDashboard.json'):
        errs.append('ETLDashboard.json file does not exist.')
    else:
        args = LoadJsonFile('ETLDashboard.json')
        missing = req_args_fixed - set(args)
        if missing:
            errs.append('The following required arguments are missing from ETLDashboard.json: %s' % ','.join(missing))
    if errs:
        raise Exception('\n'.join(errs))
    #############################
    # Required Arguments:
    #############################
    # dynamicetlservicepath:
    if not os.path.exists(args['dynamicetlservicepath']):
        errs.append('(dynamicetlservicepath) Path does not exist.')
    elif not args['dynamicetlservicepath'].endswith('.exe'):
        errs.append('(dynamicetlservicepath) Path must point to an .exe.')

    # etlfilepaths:
    if not isinstance(args['etlfilepaths'], str):
        errs.append('(etlfilepaths) Must be a string.')
    elif not args['etlfilepaths'].endswith('json'):
        errs.append('(etlfilepaths) Must point to .json file.')
    elif not os.path.exists(args['etlfilepaths']):
        errs.append('(etlfilepaths) Does not exist.')
    else:
        args['etlfilepaths'] = LoadJsonFile(args['etlfilepaths'])

    # filewatcherappsettingstemplatepath:
    if not os.path.exists(args['filewatcherappsettingstemplatepath']):
        errs.append('(filewatcherappsettingstemplatepath) Path does not exist.')
    elif not args['filewatcherappsettingstemplatepath'].endswith('.json'):
        errs.append('(filewatcherappsettingstemplatepath) Path must point to .json.')
    else:
        # Ensure filewatcher config has correct keys:
        try:
            fwconfig = LoadJsonFile(args['filewatcherappsettingstemplatepath'])
            if not 'files' in fwconfig:
                errs.append('(filewatcherappsettingstemplatepath) "files" key is missing from .json file.')
            else:
                args['filewatcher'] = fwconfig
        except Exception as ex:
            errs.append('(filewatcherappsettingstemplatepath) Issue with json file: %s' % str(ex))
            
    # logpath:
    if not os.path.isdir(args['logpath']):
        errs.append('(logpath) Must point to a folder.')
    elif not os.path.exists(args['logpath']):
        errs.append('(logpath) Folder does not exist.')

    # serviceappsettingspath:
    if not os.path.exists(args['serviceappsettingspath']):
        errs.append('(serviceappsettingspath) Does not exist.')
    elif not args['serviceappsettingspath'].endswith('.json'):
        errs.append('(serviceappsettingspath) Must point to .json file.')
    else:
        try:
            args['appsettings'] = LoadJsonFile(args['serviceappsettingspath'])
        except Exception as ex:
            errs.append('(serviceappsettingspath) Appsettings json file has following issue: %s' % str(ex))

    # webapipath:
    if not os.path.exists(args['webapipath']):
        errs.append('(webapipath) Path does not exist.')
    elif not args['webapipath'].endswith('.dll'):
        errs.append('(webapipath) Path must point to dll.')
    if errs:
        raise Exception('\n'.join(errs))

    return args

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
    """
    * Get command line arguments for GenerateColumnAttributesReport.py script.
    """
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
# GenerateFileTransferConfig.py
############################
def GenerateFileTransferConfigJsonArgs():
    """
    * Pull and validate required arguments for 
    GenerateFileTransferConfig script.
    """
    req_args = set(['groupregex'])
    errs = []
    if not os.path.exists('GenerateFileTransferConfig.json'):
        raise Exception('GenerateFileTransferConfig.json does not exist.')
    try:
        args = ETLDashboardJsonArgs()
        args.update(LoadJsonFile('GenerateFileTransferConfig.json'))
    except Exception as ex:
        errs.append('Failed to read GenerateFileTransferConfig.json')
        errs.append('Reason: %s' % str(ex))
        raise Exception('\n'.join(errs))
    missing = req_args - set(args)
    if missing:
        raise Exception('The following required arguments are missing: %s' % ','.join(missing))
    
    if not os.path.exists(os.getcwd() + '\\AppsettingsFiles'):
        errs.append('Local \\AppsettingsFiles\\ folder is missing.')
    else:
        if not os.path.exists(os.getcwd() + '\\AppsettingsFiles\\etlfilepaths.json'):
            errs.append('etlfilepaths.json file is missing.')
        else:
            args['etlpaths'] = LoadJsonFile(os.getcwd() + '\\AppsettingsFiles\\etlfilepaths.json')
        if not os.path.exists(os.getcwd() + '\\AppsettingsFiles\\config.json'):
            errs.append('Local AppsettingsFiles\\config.json file is missing.')
        else:
            args['config'] = LoadJsonFile(os.getcwd() + '\\AppsettingsFiles\\config.json')
        if 'etlpaths' in args and 'config' in args:
            args['etlpaths'] = FillEnvironmentVariables(args['etlpaths'], args['config'], "PROD")
    ############################
    # Required arguments:
    ############################
    # groupregex: 
    if not isinstance(args['groupregex'], str):
        errs.append('(groupregex) Must be a string.')
    elif not IsRegex(args['groupregex']):
        errs.append('(groupregex) Invalid regular expression,')
    if errs:
        raise Exception('\n'.join(errs))
    return args

############################
# TestETLPipeline.py
############################
def TestETLPipelineJsonArgs():
    """
    * Pull and validate arguments from local json file.
    """
    req_args = set(['etlname','filedate','postargspath','reportpath','testmode'])
    req_postargs = set(['id', 'fileid', 'subject', 'arg', 'fileName'])
    req_postargs_arg = set(['FilePath'])
    if not os.path.exists('TestETLPipeline.json'):
        raise Exception('TestETLPipeline.json does not exist.')
    args = {}
    args['testetlargs'] = LoadJsonFile('TestETLPipeline.json')
    args['fixedargs'] = ETLDashboardJsonArgs()
    # Ensure required arguments are present:
    errs = []
    missing = req_args - set(args['testetlargs'])
    if missing:
        errs.append('The following required args are missing from TestETLPipeline.json: %s' % ','.join(missing))
    if not os.path.exists(os.getcwd() + '\\AppsettingsFiles'):
        errs.append('Local \\AppsettingsFiles\\ folder is missing.')
    elif not os.path.exists(os.getcwd() + '\\AppsettingsFiles\\config.json'):
        errs.append('Local AppsettingsFiles\\config.json file is missing.')
    else:
        try:
            args['config'] = LoadJsonFile(os.getcwd() + '\\AppsettingsFiles\\config.json')
        except Exception as ex:
            errs.append('Issue with config.json: %s' % str(err))
    if errs:
        raise Exception('\n'.join(errs))
    
    ########################
    # testetlargs:
    ########################
    # postargspath:
    if not os.path.exists(args['testetlargs']['postargspath']):
        errs.append('(postargspath) Path does not exist.')
    elif not args['testetlargs']['postargspath'].endswith('.json'):
        errs.append('(postargspath) Path must point to a json file.')
    else:
        post_args = LoadJsonFile(args['testetlargs']['postargspath'])
        missing = req_postargs - set(post_args)
        if missing:
            errs.append('(postargspath) The following required arguments in json file are missing: {%s}' % ','.join(missing))
        else:
            args['testetlargs']['postargs'] = post_args
    # Get sample file name from post args file:
    if 'postargs' in args['testetlargs'] and 'arg' in args['testetlargs']['postargs']:
        match = re.search('[A-Z]:.+', args['testetlargs']['postargs']['arg'])
        if not match:
            errs.append('(postargs) arg::FilePath is not a valid path.')
        else:
            args['testetlargs']['samplefile'] = match[0].strip("{}'")
        if 'samplefile' in args['testetlargs'] and not os.path.isfile(args['testetlargs']['samplefile']):
            errs.append('(postargspath) File at arg::FilePath does not point to valid file.')

    # testmode:
    args['testetlargs']['testmode'] = args['testetlargs']['testmode'].upper()
    validModes = set(['QA', 'STG', 'UAT', 'LOCAL'])
    if args['testetlargs']['testmode'] not in validModes:
        errs.append('(testmode) Must be one of %s (not %s).' % (validModes, args['testetlargs']['testmode']))
        args['testetlargs']['testmode'] = None
    elif 'filewatcher' in args['fixedargs']:
        args['fixedargs']['filewatcher'] = FillEnvironmentVariables(args['fixedargs']['filewatcher'],args['config'],args['testetlargs']['testmode'])
    if not args['testetlargs']['testmode'] is None:
        args['testetlargs']['server'] = args['config']['Servers'][args['testetlargs']['testmode']]

    # etlname: ensure etl is present in appsettings file. 
    # If present then get etl's database and table names from the 
    # appsettings file:
    if 'appsettings' in args['fixedargs'] and not args['testetlargs']['etlname'] in args['fixedargs']['appsettings']['Etls']:
        errs.append('(etlname) ETL %s not in appsettings file.' % args['testetlargs']['etlname'])
    elif 'appsettings' in args['fixedargs']:
        etl = args['testetlargs']['etlname']
        # Ensure that service appsettings json file has all necessary keys:
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
    if not args['testetlargs']['testmode'] is None and 'filewatcher' in args['fixedargs'] and 'files' in args['fixedargs']['filewatcher']:
       # Get etl output path using filewatcher config:
       path = None
       for config in args['fixedargs']['filewatcher']['files']:
           if 'subject' in config and args['testetlargs']['etlname'] == config['subject']:
               path = config['inbound']
               break
       if path is None:     
           errs.append('(etlname) ETL not configured in filewatcher appsettings file.')
       else:
           # Fill in environment variables using config.json:
           args['testetlargs']['etlfolder'] = os.path.split(path)[0] + ('\\' if not os.path.split(path)[0].endswith('\\') else '')
           if args['testetlargs']['testmode'] != 'LOCAL' and not os.path.exists(args['testetlargs']['etlfolder']):
               errs.append('(filewatcherappsettingstemplatepath) ETL folder does not exist.')
           
    # filedate: 
    if not StringIsDT(args['testetlargs']['filedate'], False):
        errs.append('(filedate) %s is invalid date string.' % args['testetlargs']['filedate'])
    else:
        args['testetlargs']['filedate'] = StringIsDT(args['testetlargs']['filedate'], True)

    # reportpath:
    if not args['testetlargs']['reportpath'].endswith('.xlsx'):
        errs.append('(reportpath) Must point to xlsx file.')

    # comparefile (optional):
    if 'comparefile' in args['testetlargs'] and not os.path.isfile(args['testetlargs']['comparefile']):
        errs.append('(comparefile) Not a valid file.')

    # ignorecols (optional):
    if 'ignorecols' in args['testetlargs'] and not isinstance(args['testetlargs']['ignorecols'], list):
        errs.append('(ignorecols) Must be a list if provided.')

    # pkey (optional):
    if 'pkey' in args['testetlargs']:
        if isinstance(args['testetlargs']['pkey'], str):
            args['testetlargs']['pkey'] = [args['testetlargs']['pkey']]
        elif not isinstance(args['testetlargs']['pkey'], list):
            errs.append('pkey must be a string or a list.')

    if errs:
        raise Exception('\n'.join(errs))

    return args

############################
# GenerateETLInfo.py
############################
def GenerateETLInfoJsonArgs():
    """
    * Get and verify arguments from ViewETLInfo.json.
    """
    reqArgs = set(['etlname','summarypath'])
    args = None
    errs = []
    if not os.path.exists('ViewETLInfo.json'):
        errs.append('ViewETLInfo.json does not exist.')
    else:
        try:
            args = json.load(open('ViewETLInfo.json', 'rb'))
            args.update(ETLDashboardJsonArgs())
        except Exception as ex:
            errs.append('Could not load ViewETLInfo.json. Reason: %s' % str(ex))
        
    if not args is None:
        missing = reqArgs - set(args)
        if missing:
            errs.append('The following required args are missing: %s' % ','.join(missing))
    ##################################
    # Required Arguments:
    ##################################
    # summarypath:
    if 'summarypath' in args and not args['summarypath'].endswith('.xlsx'):
        errs.append('(summarypath) Must point to .xlsx file.')

    ##################################
    # Optional:
    ##################################
    # openfileexplorerpaths:
    if 'openfileexplorerpaths' in args:
        args['openfileexplorerpaths'] = args['openfileexplorerpaths'].lower()
        if not args['openfileexplorerpaths'] in ['true', 'false']:
            errs.append('(openfileexplorerpaths) Must be true/false (case insensitive).')
        elif args['openfileexplorerpaths'] == 'true':
            args['openfileexplorerpaths'] = True
        else:
            args['openfileexplorerpaths'] = False
            
    if errs:
        raise Exception('\n'.join(errs))
    
    return args