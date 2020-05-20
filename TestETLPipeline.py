#####################################
# TestETLPipeline.py
#####################################
# Description:
# 1) Open test version of WebAPI, execute post request using sample data file to test
# ETL pipeline on local MetricsDYETL database. 
# 2) Execute DynamicETL.Service to run ETL pipeline with local database.
# 3) Query local MetricsDYETL database, compare uploaded contents to original sample file.
# 4) Generate report accounting for exceptions thrown by DynamicETL.Service or any source data versus
# uploaded data discrepancies.

from ETL.ETLJobLoader import ETLJobLoader
from ETL.DataComparer import DataComparer
from ETL.TSQLInterface import TSQLInterface
import json
import os
import sys

def LoadArgsFromJSON():
    """
    * Pull and validate arguments from local json file.
    """
    req_args = ['dynamicetlservicepath','logpath','webapiurl','postargspath','reportpath','sqlconnection','sqldatabase','sqltable','webapipath'] 
    req_args = set(req_args)
    req_postargs = set(['id', 'fileid', 'subject', 'arg', 'fileName'])
    req_postargs_arg = set(['FilePath'])
    args = json.load(open('TestETLPipeline.json', 'rb'))
    
    # Validate arguments:
    errs = []
    missing = req_args - set(args)
    if missing:
        errs.append('The following required args are missing: {%s}' % ','.join(missing))
        raise Exception(''.join(errs))

    # reportpath:
    if not args['reportpath'].endswith('.xlsx'):
        errs.append('(reportpath) Must point to xlsx file.')

    # logpath:
    if not os.path.isdir(args['logpath']):
        errs.append('(logpath) Must point to a folder.')
    elif not os.path.exists(args['logpath']):
        errs.append('(logpath) Folder does not exist.')

    # postargspath:
    if not os.path.exists(args['postargspath']):
        errs.append('(postargspath) Path does not exist.')
    elif not args['postargspath'].endswith('.json'):
        errs.append('(postargspath) Path must point to a json file.')
    else:
        post_args = json.load(open(args['postargspath'], 'rb'))
        missing = req_postargs - set(post_args)
        if missing:
            errs.append('(postargspath) The following required arguments in json file are missing: {%s}' % ','.join(missing))
        else:
            args['postargs'] = post_args

    # webapipath:
    if not os.path.exists(args['webapipath']):
        errs.append('(webapipath) Path does not exist.')
    elif not args['webapipath'].endswith('.dll'):
        errs.append('(webapipath) Path must point to dll.')

    # dynamicetlservicepath:
    if not os.path.exists(args['dynamicetlservicepath']):
        errs.append('(dynamicetlservicepath) Path does not exist.')
    elif not args['dynamicetlservicepath'].endswith('.exe'):
        errs.append('(dynamicetlservicepath) Path must point to dll.')

    if errs:
        raise Exception('\n'.join(errs))

    return args

def TestETLPipeline():
    print("------------------------------")
    print("TestETLPipeline: ")
    print("------------------------------")
    # Pull and verify script parameters:
    try:
        args = LoadArgsFromJSON()
    except Exception as ex:
        msg = '%s:\n%s' % ('The following input argument issues occured:', str(ex))
        print(msg)
        sys.exit()
    # Open DynamicETL.WebApi and post test ETL job:
    print("Loading ETL %s test job to WebAPI at" % args['postargs']['subject'])
    print(args['webapipath'])
    loader = ETLJobLoader(args['webapipath'],args['dynamicetlservicepath'],args['logpath'],args['webapiurl'])
    loader.RunETL(args['postargs'])
    # Determine if any issues occurred in the log file. Exit application if issues occurred:
    message = loader.ReadLogFile()
    if message:
        print(message)
        sys.exit()
    # Query server to get uploaded data:
    TSQLInterface(args['sqlconnection'], args['sqldatabase'])

    # Compare input versus output etl data:
    tester = DataComparer()
    tester.GenerateComparisonReport(args['reportpath'],)


if __name__ == "__main__":
    TestETLPipeline()