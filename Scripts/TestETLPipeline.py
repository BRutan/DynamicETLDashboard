#####################################
# TestETLPipeline.py
#####################################
# Description:
# * If 'testmode' is "LOCAL":
# 1) Open test version of WebAPI, execute post request using arguments specified in
# postargs.json. 
# 2) Execute DynamicETL.Service to run ETL pipeline with local database.
# 3) Query local MetricsDYETL database, compare uploaded contents to original sample file.
# 4) Generate report accounting for exceptions thrown by DynamicETL.Service or any source data versus
# uploaded data discrepancies.
# * else if 'testmode' is in ['QA','UAT','STG']:
# 1) Drop source file specified in postargs.json at ETL drop path based upon test mode.
# 2) Query relevant table in MetricsDyetl at server based upon mode after data has been
# sucked up.
# 3) Generate report accounting for differences between data in table versus
# source file.

from datetime import datetime
from ETL.DataComparer import DataComparer
from ETL.DataReader import DataReader
from ETL.ETLJobLoader import ETLJobLoader
from ETL.TSQLInterface import TSQLInterface
from Logging.DynamicETLServiceIssueParser import DynamicETLServiceIssueParser
import json
import os
from shutil import copyfile
import sys
from time import sleep
from Utilities.FixJsonConfigs import FixJsonConfigs
from Utilities.Helpers import Countdown
from Utilities.LoadArgs import TestETLPipelineJsonArgs

def TestETLPipeline():
    """
    * Perform key steps in order.
    """
    print ("------------------------------")
    print ("TestETLPipeline: ")
    print ("------------------------------")
    # Get parameters from .json files:
    args, argTup = GetParameters()
    # Connect to T-SQL instance:
    interface = ConnectToServer(args, argTup)
    # Drop sample file to correct location, or if testing locally, run DYETL.WebApi, post arguments
    # and run DYETL.Service:
    waittime = OutputSampleFile(args, argTup, interface)
    pkeys, ignorecols, data_test, data_valid = GetDatasets(args, argTup, interface, waittime)
    # Output report detailing difference between file data and data in table:
    GenerateReport(args, pkeys, ignorecols, data_test, data_valid)

def GetParameters():
    """
    * Validate and get parameters from local
    json files.
    """
    # Ensure all AppSettings json files can be uploaded:
    #FixJsonConfigs()
    # Pull and verify script parameters:
    try:
        args = TestETLPipelineJsonArgs()
    except Exception as ex:
        msg = '%s:\n%s' % ('The following input argument issues occured:', str(ex))
        print(msg)
        input('Press enter to exit.')
        os._exit(0)
    # (0, 1, 2, 3, 4)
    # (FileDateVal, Server, DataBase, TableName, FileDateColumnName)
    argTup = (args['testetlargs']['filedate'].strftime('%Y-%m-%d'),args['testetlargs']['server'],args['testetlargs']['database'],args['testetlargs']['tablename'],args['testetlargs']['filedatecolname'])
    return args, argTup

def ConnectToServer(args, argTup):
    """
    * Connect to target T-SQL server and database. 
    If testing LOCAL:
    * Drop existing rows with test FileDate.
    * Open DYETL.WebApi, post file using postargs.json.
    * Run DYETL.Service to insert data into target table.
    * Check local log file, throw exception if issue occurred with
    DYETL.Service.
    If not testing STG:
    * Drop existing rows with test FileDate.
    """
    try:
        interface = TSQLInterface(argTup[1], argTup[2])
    except Exception as ex:
        print ("Could not connect to %s::%s" % (argTup[1], argTup[2]))
        print ("Reason: %s" % str(ex))
        input ('Press enter to exit.')
        os._exit(0)
    if args['testetlargs']['testmode'] == 'LOCAL':
        # Open DynamicETL.WebApi and post test ETL job:
        print ("Loading ETL %s test job to WebAPI at" % args['testetlargs']['postargs']['subject'])
        print (args['fixedargs']['webapipath'])
        try:
            loader = ETLJobLoader(args['fixedargs']['webapipath'],args['fixedargs']['dynamicetlservicepath'],args['fixedargs']['logpath'],args['fixedargs']['webapiurl'])
            loader.RunETL(args['testetlargs']['postargs'])
            # Check log file for issues, throw exception if occurred:

        except Exception as ex:
            print ('ETL could not be run. Reason: %s' % str(ex))
            input ('Press enter to exit.')
            os._exit(0)
    elif args['testetlargs']['testmode'] != 'STG' and args['testetlargs']['removeprevfiledate']:
        # Remove data with filedate from server:
        print('Removing data with [%s] %s from %s::%s::%s' % (argTup[4], argTup[0], argTup[1], argTup[2], argTup[3]))
        try:
            query = "DELETE FROM [%s] WHERE [%s] = '%s';" % (argTup[3], argTup[4], argTup[0])
            interface.Execute(query)
        except Exception as ex:
            print ('Could not delete data with [%s] %s from %s::%s::%s.' % argTup[4], argTup[1], argTup[2], argTup[3])
            print ('Reason: %s' % str(ex))
            input ('Press enter to exit.')
            os._exit(0)

    return interface

def OutputSampleFile(args, argTup, interface):
    """
    * Output sample file to FileWatcher folder, wait for sample file to be sucked
    up by ETL. If does not suck up, notify user:
    """
    waittime = 40
    print ('Outputting data file to')
    print ('%s' % args['testetlargs']['etlfolder'])
    print ('Will wait up to %d seconds to allow data to be implemented...' % waittime)
    sampleFileName = os.path.split(args['testetlargs']['samplefile'])[1]
    filewatcherPath = "%s%s" % (args['testetlargs']['etlfolder'], sampleFileName)
    copyfile(args['testetlargs']['samplefile'], filewatcherPath)
    startTime = datetime.now()
    Countdown(waittime, lambda path = filewatcherPath : not os.path.exists(filewatcherPath))
    interval = (startTime, datetime.now())
    if os.path.exists(filewatcherPath):
        print ('File was not implemented into ETL %s after %d seconds.' % (args['testetlargs']['etlname'], waittime))
        input ('Press enter to exit.')
        os._exit(0)
    elif os.path.exists(args['testetlargs']['logpath']):
        # Notify user if any DynamicETL.Service issues occured in logfile:
        issues = DynamicETLServiceIssueParser(args['testetlargs']['logpath']).ETLHasIssues(args['testetlargs']['etlname'], interval)
        if not issues is None:
            print ("The following issues occurred in DynamicETL.Service: %s" % issues)
            input ('Press enter to exit.')
            os._exit(0)
    return waittime

def GetDatasets(args, argTup, interface, waittime):
    """
    Query server to get test data, pull
    valid data from local file.
    """
    try:
        query = "SELECT * FROM [%s] WHERE [%s] = '%s'" % (argTup[3], argTup[4], argTup[0])
        print ('Waiting %d seconds to allow data to be pulled and transformed...' % waittime)
        # Keep pulling from server until data has been uploaded:
        Countdown(waittime)
        data_test = interface.Select(query)
        if len(data_test) == 0:
            raise Exception('No data uploaded to server after dropping to etlfolder.')
    except Exception as ex:
        print ('Could not query %s::%s::%s' % (argTup[1], argTup[2], argTup[3]))
        print ('Reason: %s' % str(ex))
        input ('Press enter to exit.')
        os._exit(0)
    # Pull data from test file:
    compareFile = args['testetlargs']['comparefile'] if 'comparefile' in args['testetlargs'] else args['testetlargs']['samplefile']
    data_valid = DataReader.Read(compareFile, delim = args['testetlargs']['delim'])
    # Compare test file data versus output etl data:
    print('Generating comparison report...')
    ignorecols = ['%s' % argTup[4], 'RunDate']
    if 'ignorecols' in args['testetlargs']:
        ignorecols.extend(args['testetlargs']['ignorecols'])
    ignorecols = set([col.strip() for col in ignorecols if col.strip()])
    if 'pkey' in args['testetlargs']:
        pkeys = args['testetlargs']['pkey'] 
    else:
        print ("Finding appropriate primary key(s) to compare datasets using input file...")
        pkeys = TSQLInterface.PrimaryKeys(data_valid, 4, ignorecols, True)
        print ("Using: {%s} as primary key(s)..." % ', '.join(pkeys))

    return pkeys, ignorecols, data_test, data_valid

def GenerateReport(args, pkeys, ignorecols, data_test, data_valid):
    """
    * Generate report comparing input data and data in server.
    """
    tester = DataComparer()
    try:
        tester.GenerateComparisonReport(args['testetlargs']['reportpath'], data_test, data_valid, ignorecols, pkeys)
    except Exception as ex:
        print ('Could not generate report.')
        print ('Reason: %s' % str(ex))
        input ('Press enter to exit.')
        os._exit(0)
    print ('Finished generating report at')
    print (args['testetlargs']['reportpath'])

if __name__ == "__main__":
    TestETLPipeline()