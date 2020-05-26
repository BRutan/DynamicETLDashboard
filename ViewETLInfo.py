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
    # Aggregate and display information:
    info = ETLInfo(args['etlname'], args)
    print(info.Summarize())



if __name__ == '__main__':
    ViewETLInfo()