#####################################
# ViewETLInfo.py
#####################################
# Description:
# * Look up information regarding ETLs.
# (data folders for QA/UAT/STG, tablename on SQL Server, ).

from ETL.ETLInfo import ETLInfo
import os
from Utilities.LoadArgs import GenerateETLInfoJsonArgs

def ViewETLInfo():
    """
    * Generate report with information regarding etls.
    """
    print("------------------------------")
    print("GenerateETLInfoJsonArgs: ")
    print("------------------------------")
    try:
        args = GenerateETLInfoJsonArgs()
    except Exception as ex:
        msg = 'The following argument issues occurred:\n%s' % str(ex)
        print(msg)
        os._exit(0)
    # Aggregate info, generate report and display information:
    info = ETLInfo(args['etlname'], args)
    info.GenerateSummaryReport(args['summarypath'])
    print(info.Summarize())


if __name__ == '__main__':
    ViewETLInfo()