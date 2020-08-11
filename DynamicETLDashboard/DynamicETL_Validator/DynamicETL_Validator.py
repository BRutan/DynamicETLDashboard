#####################################
# DynamicETL_Validator.py
#####################################
# Description:
# * Run all configured APIs to perform
# ETL validation.

from DynamicETL_Dashboard.Logging.ScriptLogger import ScriptLogger
from Configs.ValidatorConfig import ValidatorConfig
from Utilities.LoadArgs import DynamicETL_ValidatorJsonArgs
import sys

# https://levelup.gitconnected.com/python-dependency-injection-with-flask-injector-50773d451a32

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
        args = DynamicETL_ValidatorJsonArgs()
    except Exception as ex:
        msg = "Error with RunAPIs.json: %s." % str(ex)
        log.Exception(msg)
        sys.exit(0)

    return log, args

def GetConfigs(log, args):
    """
    * Read RunAPIs.json to get all 
    configured APIs.
    """
    reader = ValidatorConfig("")

def RunAllAPIs(log, args, configs):
    """
    * Run all generated APIs.
    """
    pass

if __name__ == '__main__':
    RunAPIs()