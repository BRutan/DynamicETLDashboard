#####################################
# TestETLPipeline.py
#####################################
# Description:
# 1) Open test version of WebAPI, execute post request using args specified in
# postargs.json. 
# 2) Execute DynamicETL.Service to run ETL pipeline with local database.
# 3) Query local MetricsDYETL database, compare uploaded contents to original sample file.
# 4) Generate report accounting for exceptions thrown by DynamicETL.Service or any source data versus
# uploaded data discrepancies.

from ETL.DataComparer import DataComparer
from ETL.DataReader import DataReader
from ETL.ETLJobLoader import ETLJobLoader
from ETL.TSQLInterface import TSQLInterface
import json
import os
from shutil import copyfile
import sys
from time import sleep
from Utilities.FixJsonConfigs import FixJsonConfigs
from Utilities.Helpers import Countdown
from Utilities.LoadArgs import TestETLPipelineJsonArgs

def TestETLPipeline():
    print("------------------------------")
    print("TestETLPipeline: ")
    print("------------------------------")
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
    if args['testetlargs']['testmode'] == 'LOCAL':
        # Open DynamicETL.WebApi and post test ETL job:
        print("Loading ETL %s test job to WebAPI at" % args['postargs']['subject'])
        print(args['webapipath'])
        try:
            loader = ETLJobLoader(args['webapipath'],args['dynamicetlservicepath'],args['logpath'],args['webapiurl'])
            loader.RunETL(args['postargs'])
        except Exception as ex:
            print('ETL could not be run:')
            print(str(ex))
            input('Press enter to exit.')
            os._exit(0)
        # Determine if any issues occurred in the WebAPI/Service log file.
        # Exit application if issues occurred:
        messages = []
        messages.append(loader.ReadLogFile())
        messages.append(loader.ReadLogFile())
        if message:
            print(message)
            input('Press enter to exit.')
            os._exit(0)
    else:
        # Output sample file to FileWatcher folder, wait for sample file to be sucked
        # up by etl. If does not suck up, delete file and notify user:
        print('Outputting data file to')
        print('%s' % args['testetlargs']['etlfolder'])
        print('Will wait five seconds to allow data to be implemented...')
        sampleFileName = os.path.split(args['testetlargs']['samplefile'])[1]
        filewatcherPath = "%s%s" % (args['testetlargs']['etlfolder'],sampleFileName) 
        copyfile(args['testetlargs']['samplefile'], filewatcherPath)
        Countdown(5)
        if os.path.exists(filewatcherPath):
            print('File was not implemented into etl after 5 seconds.')
            input('Press enter to exit.')
            os._exit(0)
    # Query server to get uploaded data:
    try:
        interface = TSQLInterface(args['testetlargs']['server'], args['testetlargs']['database'])
        query = "SELECT * FROM %s WHERE [fileDate] = %s" % (args['testetlargs']['tablename'], args['testetlargs']['filedate'].strftime('%Y-%m-%d'))
        data_test = interface.Select(query)
    except Exception as ex:
        print('Could not query %s::%s::%s' % (args['testetlargs']['server'], args['testetlargs']['database'], args['testetlargs']['tablename']))
        print('Reason: %s' % str(ex))
        input('Press enter to exit.')
        os._exit(0)
    # Pull data from test file:
    data_valid = DataReader.Read(args['testetlargs']['samplefile'])
    # Compare test file data versus output etl data:
    print('Generating comparison report...')
    tester = DataComparer()
    ignoreCols = ['FileDate', 'RunDate']
    if 'ignorecols' in args['testetlargs']:
        ignoreCols.extend(args['testetlargs']['ignorecols'])
    tester.GenerateComparisonReport(args['testetlargs']['reportpath'], data_test, data_valid, ignoreCols)
    print('Finished generating report at')
    print(args['testetlargs']['reportpath'])

if __name__ == "__main__":
    TestETLPipeline()