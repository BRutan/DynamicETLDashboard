#####################################
# RunAPIs.py
#####################################
# Description:
# * Run all configured APIs defined in Data namespace.

from Data.APIConfigReader import APIConfigReader
from Data.APIFactory import APIFactory
from Logging.ScriptLogger import ScriptLogger
import sys
from Utilities.LoadArgs import RunAPIsJsonArgs

def RunAPIs():
    """
    * Perform key steps in order.
    """
    args, log = Initialize()
    configs = GetConfigs(log, args)
    RunAllAPIs(log, args, configs)

def Initialize():
    """
    * Pull arguments from RunApis.json and get log file.
    """
    log = ScriptLogger()
    try:
        args = RunAPIsJsonArgs()
    except Exception as ex:
        msg = "Error with RunAPIs.json: %s." % str(ex)
        log.Exception(msg)
        sys.exit(0)

    return log, args

def GetConfigs(log, args):
    """
    * Read RunAPIs.json to get all configured
    APIs.
    """
    reader = APIConfigReader()

def RunAllAPIs(log, args, configs):
    """
    * Run all generated APIs.
    """
    pass

if __name__ == '__main__':
    RunAPIs()