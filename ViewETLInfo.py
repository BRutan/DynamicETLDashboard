#####################################
# ViewETLInfo.py
#####################################
# Description:
# * Look up information regarding ETLs.
# (data folders for QA/UAT/STG, tablename on SQL Server, ).

from ETL.ETLInfo import ETLInfo
from Utilities.LoadArgs import ViewETLInfoJsonArgs

def ViewETLInfo():
    """
    * Display useful information regarding etlname.
    """
    args = ViewETLInfoJsonArgs()
    # Aggregate info, generate report and display information:
    info = ETLInfo(args['etlname'], args)
    info.GenerateSummaryReport(args['summarypath'])
    print(info.Summarize())


if __name__ == '__main__':
    ViewETLInfo()