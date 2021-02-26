#####################################
# PullData.py
#####################################
# Description:
# * Pull data using query, store in file.


from Database.tsql import TSQLInterface
from Utilities.LoadArgs import PullDataJsonArgs

def PullData():
    """
    * 
    """
    try:
        args = PullDataJsonArgs()
    except Exception as ex:
        print("")

    

