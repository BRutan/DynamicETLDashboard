#####################################
# PullData.py
#####################################
# Description:
# * Pull data using query, store in file.


from Tables.TSQLInterface import TSQLInterface
from Utilities.LoadArgs import PullDataJsonArgs

def PullData():
    """
    * 
    """
    try:
        args = PullDataJsonArgs()
    except Exception as ex:
        print("")

    

