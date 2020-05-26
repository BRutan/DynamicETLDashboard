#####################################
# ViewETLInfo.py
#####################################
# Description:
# * Look up information regarding ETLs.
# (data locations for QA/UAT/STG, tablename on SQL Server,
# ).

from Utilities.LoadArgs import ViewETLInfoJsonArgs

def ViewETLInfo():
    """
    * Display useful information regarding etlname.
    """
    args = ViewETLInfoJsonArgs()



if __name__ == '__main__':
    ViewETLInfo()


