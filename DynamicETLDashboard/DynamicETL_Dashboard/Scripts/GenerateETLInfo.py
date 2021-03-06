#####################################
# GenerateETLInfo.py
#####################################
# Description:
# * Generate report detailing useful information regarding ETLs.
# (data folders for QA/UAT/STG, tablename on SQL Server, ).

from ETL.ETLInfo import ETLInfo
import os
from Utilities.LoadArgs import GenerateETLInfoJsonArgs

def GenerateETLInfo():
    """
    * Generate report with information regarding etls.
    """
    print("------------------------------")
    print("GenerateETLInfo: ")
    print("------------------------------")
    try:
        args = GenerateETLInfoJsonArgs()
    except Exception as ex:
        msg = 'The following argument issues occurred:\n%s' % str(ex)
        print(msg)
        os._exit(0)
    # Generate FileTransfer json file if does not exist:
    if not os.path.exists():
        pass
    # Aggregate info, generate report and display information:
    try:
        info = ETLInfo(args['etlname'], args['config'], args['etlfilepaths'], args['filewatcher'], args['serviceappsettings'])
        info.GenerateSummaryReport(args['summarypath'])
        print(info.Summarize())
    except Exception as ex:
        pass
    
if __name__ == '__main__':
    GenerateETLInfo()