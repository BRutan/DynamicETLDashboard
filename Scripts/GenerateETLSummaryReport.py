#####################################
# GenerateETLSummaryReport.py
#####################################
# Description:
# * Generate report (defined in ETLSummaryReport class) 
# detailing differences between input data sources and 
# data loaded into ETL warehouse.

from argparse import ArgumentParser, ArgumentError
import dateutil.parser as dtparser
from Guggenheim.Reports.ETLSummaryReport import ETLSummaryReport
from Logging.ScriptLogger import ScriptLogger
import os
from Utilities.LoadArgs import GenerateETLSummaryReportJsonArgs
from Utilities.Helpers import StringIsDT
import sys
import warnings

def GenerateETLSummaryReport():
    """
    * Perform key steps in generating
    ETL summary report following DynamicEtl.Service 
    run.
    """
    print ("------------------------------")
    print ("GenerateETLSummaryReport: ")
    print ("------------------------------")
    args, scriptArgs, log = GetArguments()
    GenerateReport(args, log)

def GetArguments():
    """
    * Get arguments from command line and script json file.
    """
    # Get arguments unique to script:
    try:
        scriptArgs = GenerateETLSummaryReportJsonArgs()
        log = ScriptLogger(scriptArgs['logpath'], 'GenerateETLSummaryReport')
    except Exception as ex:
        errs.extend(str(ex).split('\n'))
        if errs:
            print("The following argument issues occurred:")
            for err in errs:
                print(err)
            sys.exit(0)

    checkDate = lambda dt : StringIsDT(dt, True)
    parser = ArgumentParser("GenerateETLSummaryReport")
    parser.add_argument("etl", type=str, help = "Full name of ETL configured in DynamicEtl.Service.")
    parser.add_argument("status", type=str, help = "Full name of ETL configured in DynamicEtl.Service.")
    parser.add_argument("inputfilepath", type=str, help = "Full path of sample file used to input data.")
    parser.add_argument("filedate", type=checkDate, help = "Date used in file title.")
    parser.add_argument("tablename", type=str, help = "T-SQL tablename that ETL inserts data into.")
    parser.add_argument("database", type=str, help = "Database where tablename lives.")
    parser.add_argument("server", type=str, help = "Server where tablename and database live.")
    parser.add_argument("reportdest", type=str, help = "Full path destination for ETL summary report.")
    parser.add_argument("starttime", type=checkDate, help = "Date + time ETL started.")
    parser.add_argument("endtime", type=checkDate, help = "Date + time ETL ended.")
    parser.add_argument("--pre", nargs='*', help = "Preoperations performed on dataset before insertion into table.")
    parser.add_argument("--input", nargs='*', help = "Inputoperations performed on dataset before insertion into table.")
    parser.add_argument("--post", nargs='*', help = "Postoperations performed on dataset after insertion into table.")
    parser.add_argument("--delim", type=str, help = "Delimiter used to parse source file.")

    errs = []
    try:
        args = parser.parse_args()
    except ArgumentError as ex:
        errs.append('Issue with following arguments: %s' % str(ex))
    except SystemExit as ex:
        errs.append('Issue with following arguments: %s' % str(ex))
    except Exception as ex:
        errs.append("Could not parse arguments from command line. Reason: %s" % str(ex))
    if errs:
        log.Error('Error with argument parsing:')
        log.Error('\n'.join(errs))
        sys.exit(0)
    
    args = vars(args)
    msg = 'Pulling in arguments for ETL %s run on %s.' % (args['etl'], args['endtime'])
    log.Info(msg)
    print(msg)
    return args, scriptArgs, log

def GenerateReport(args, log):
    """
    * Generate report detailing differences between 
    input file and table data.
    """
    try:
        report = ETLSummaryReport(**args)
        report.GenerateReport(args['reportdest'])
    except Exception as ex:
        msg = 'Could not generate report.'
        print(msg)
        log.Error(msg)
        msgs = str(ex).split('\n')
        for msg in msgs:
            log.Error(msg)
        sys.exit(0)

    msg = 'Finished generating report at %s.' % args['reportdest']
    log.Info(msg)
    print(msg)

if __name__ == '__main__':
    GenerateETLSummaryReport()