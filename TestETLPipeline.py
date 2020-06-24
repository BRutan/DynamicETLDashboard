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
    print ("------------------------------")
    print ("TestETLPipeline: ")
    print ("------------------------------")
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
    argTup = (args['testetlargs']['filedate'].strftime('%Y-%m-%d'),args['testetlargs']['server'],args['testetlargs']['database'],args['testetlargs']['tablename'])
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
        except Exception as ex:
            print ('ETL could not be run:')
            print (str(ex))
            input ('Press enter to exit.')
            os._exit(0)
        # Determine if any issues occurred in the WebAPI/Service log file.
        # Exit application if issues occurred:
        messages = []
        messages.append(loader.ReadLogFile())
        messages.append(loader.ReadLogFile())
        if message:
            print (message)
            input ('Press enter to exit.')
            os._exit(0)
    elif args['testetlargs']['testmode'] != 'STG' and args['testetlargs']['removeprevfiledate']:
        # Remove data with filedate from server:
        print('Removing data with fileDate %s from %s::%s::%s' % argTup)
        try:
            query = "DELETE FROM [%s] WHERE fileDate = '%s';" % (argTup[3], argTup[0])
            interface.Execute(query)
        except Exception as ex:
            print ('Could not delete data with fileDate %s from %s::%s::%s.' % argTup)
            print ('Reason: %s' % str(ex))
            input ('Press enter to exit.')
            os._exit(0)
    # Output sample file to FileWatcher folder, wait for sample file to be sucked
    # up by ETL. If does not suck up, notify user:
    waittime = 40
    print ('Outputting data file to')
    print ('%s' % args['testetlargs']['etlfolder'])
    print ('Will wait up to %d seconds to allow data to be implemented...' % waittime)
    sampleFileName = os.path.split(args['testetlargs']['samplefile'])[1]
    filewatcherPath = "%s%s" % (args['testetlargs']['etlfolder'], sampleFileName)
    copyfile(args['testetlargs']['samplefile'], filewatcherPath)
    Countdown(waittime, lambda path = filewatcherPath : not os.path.exists(filewatcherPath))
    if os.path.exists(filewatcherPath):
        print ('File was not implemented into ETL %s after %d seconds.' % (args['testargs']['etlname'], waittime))
        input ('Press enter to exit.')
        os._exit(0)
    # Query server to get uploaded data:
    try:
        query = "SELECT * FROM [%s] WHERE [fileDate] = '%s'" % (argTup[3], argTup[0])
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
    data_valid = DataReader.Read(compareFile)
    # Compare test file data versus output etl data:
    print('Generating comparison report...')
    tester = DataComparer()
    ignoreCols = ['FileDate', 'RunDate']
    if 'ignorecols' in args['testetlargs']:
        ignoreCols.extend(args['testetlargs']['ignorecols'])
    ignoreCols = set([col.strip() for col in ignoreCols if col.strip()])
    pkeys = args['testetlargs']['pkey'] if 'pkey' in args['testetlargs'] else None
    try:
        tester.GenerateComparisonReport(args['testetlargs']['reportpath'], data_test, data_valid, ignoreCols, pkeys)
    except Exception as ex:
        print ('Could not generate report.')
        print ('Reason: %s' % str(ex))
        input ('Press enter to exit.')
        os._exit(0)
    print ('Finished generating report at')
    print (args['testetlargs']['reportpath'])

if __name__ == "__main__":
    TestETLPipeline()