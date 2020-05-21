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

from ETL.DataComparer import DataComparer
from ETL.DataReader import DataReader
from ETL.ETLJobLoader import ETLJobLoader
from ETL.TSQLInterface import TSQLInterface
from Utilities.LoadArgs import TestETLPipelineJsonArgs
import json
import os
import sys

def TestETLPipeline():
    print("------------------------------")
    print("TestETLPipeline: ")
    print("------------------------------")
    # Pull and verify script parameters:
    try:
        args = TestETLPipelineJsonArgs()
    except Exception as ex:
        msg = '%s:\n%s' % ('The following input argument issues occured:', str(ex))
        print(msg)
        sys.exit()
    if args['testetlargs']['localtest']:
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
    else:
        # Output data to ETL pipeline folder:
        pass
    # Query server to get uploaded data:
    interface = TSQLInterface(args['sqlconnection'], args['sqldatabase'])
    query = "SELECT * FROM %s" % args['sqltablename']
    data_test = interface.Select(query)
    # Pull data from test file:
    data_valid = DataReader.Read(args['samplefile'])
    # Compare input versus output etl data:
    tester = DataComparer()
    tester.GenerateComparisonReport(args['reportpath'], data_test, data_valid, ['FileDate', 'RunDate'])

if __name__ == "__main__":
    TestETLPipeline()