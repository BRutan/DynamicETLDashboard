#####################################
# DynamicETL_Validator.py
#####################################
# Description:
# * Run all configured APIs to perform
# ETL validation.

# Import the AllConfigs module to store all Configs objects to dynamically inject into APIs:
#from APIs.ETLSummaryReportAPI import 
from APIs.FlaskAPIFactory import FlaskAPIFactory
from APIs.MiscControllers import healthcheck
from APIs.RegisterEndpoints import register_endpoints
import Configs.AllConfigs 
from Configs.ValidatorConfig import ValidatorConfig
from DependencyInjector.DI import inject_api_dependencies
from DynamicETL_Dashboard.Logging.ScriptLogger import ScriptLogger
from Utilities.LoadArgs import DynamicETLValidatorJsonArgs
from Reports.ETLSummaryReport import ETLSummaryReport
import sys

def RunAPIs():
    """
    * Perform key steps in order.
    """
    args = GetArgs()
    config, log = GetConfig(args)
    injector = SetInjector(args, config, log)
    endpoints = GetEndpoints(args, log)
    ConfigureAndRunFlask(args, log, config, endpoints, injector)

def GetArgs():
    """
    * Pull arguments from RunApis.json and get log file.
    """
    try:
        args = DynamicETLValidatorJsonArgs()
    except Exception as ex:
        msg = "Error with DynamicETL_Validator.json: %s." % str(ex)
        sys.exit(0)

    return args

def GetConfig(args):
    """
    * Get ValidatorConfig object from json file.
    """
    try:
        config = ValidatorConfig(args['apijson'])
    except Exception as ex:
        msg = "Could not pull config from %s. Reason: %s." % (args['apijson'], str(ex))
        sys.exit(0)
    log = ScriptLogger(args['logpath'], config.AppConfig.AppName)
    
    return config, log

def SetInjector(args, config, log):
    """
    * Setup dependency injector function for use
    with Flask application.
    """
    objs = [log]
    inj_kwargs = { 'path' : r'Configs\APIArgs.json' }
    parentCfg = ValidatorConfig
    allConfigs = 'Configs.AllConfigs'
    injector = lambda x, parentCfg = parentCfg, allConfigs = allConfigs, objs = objs, kwargs = inj_kwargs : inject_api_dependencies(x, parentCfg, allConfigs, *objs, **kwargs)
    return injector

def GetEndpoints(args, log):
    """
    * Return list of all endpoints and blueprints 
    to add to the FlaskAPIFactory.
    """
    # List of tuples with (func, endpoint, route, methods, injection, handler, **options):
    return register_endpoints()

def ConfigureAndRunFlask(args, log, config, endpoints, injector):
    """
    * Set up dependency injection and Flask application.
    """
    # Set up Flask application:
    kwargs = { 'appname' : config.AppConfig.AppName, 
               'hostname' : config.AppConfig.Hostname, 
               'debug' : args['debug'], 'injector' : injector }
    try:
        factory = FlaskAPIFactory(**kwargs)
        for arg in endpoints:
            factory.AddEndpoint(**arg)
        factory.Run()
    except Exception as ex:
        log.Error('Failed to run Flask application. Reason: %s.' % str(ex))
        sys.exit(0)
    
if __name__ == '__main__':
    RunAPIs()