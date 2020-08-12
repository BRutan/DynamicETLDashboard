#####################################
# DynamicETL_Validator.py
#####################################
# Description:
# * Run all configured APIs to perform
# ETL validation.

# Import the AllConfigs module to store all Configs objects to dynamically inject into APIs:
import Configs.AllConfigs 
from Configs.ValidatorConfig import ValidatorConfig
from DependencyInjector.DI import inject_api_dependencies
from DynamicETL_Dashboard.Logging.ScriptLogger import ScriptLogger
from flask import Flask
from flask_injector import FlaskInjector
from Utilities.LoadArgs import DynamicETLValidatorJsonArgs
import sys

# https://levelup.gitconnected.com/python-dependency-injection-with-flask-injector-50773d451a32

def RunAPIs():
    """
    * Perform key steps in order.
    """
    args, log = Initialize()
    ConfigureAndRunFlask(args, log)

def Initialize():
    """
    * Pull arguments from RunApis.json and get log file.
    """
    try:
        args = DynamicETLValidatorJsonArgs()
    except Exception as ex:
        msg = "Error with RunAPIs.json: %s." % str(ex)
        log.Exception(msg)
        sys.exit(0)

    logpath = r"C:\Users\berutan\Desktop\Projects\DynamicETLDashboard\DynamicETLDashboard\DynamicETL_Dashboard"
    log = ScriptLogger(logpath, 'DynamicETL_Validator')
    return args, log

def ConfigureAndRunFlask(args, log):
    """
    * Set up dependency injection and Flask application.
    """
    objs = [log]
    kwargs = { 'path' : r'Configs\APIArgs.json' }
    parentCfg = ValidatorConfig
    allConfigs = 'Configs.AllConfigs'
    diLambda = lambda x, parentCfg = parentCfg, allConfigs = allConfigs, objs = objs, kwargs = kwargs : inject_api_dependencies(x, parentCfg, allConfigs, *objs, **kwargs)
    app = Flask('DynamicETL_Validator')
    injector = FlaskInjector(app=app, modules=[diLambda])
    app.run()

if __name__ == '__main__':
    RunAPIs()