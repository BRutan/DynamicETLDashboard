#####################################
# LoadArgs.py
#####################################
# Description:
# * Define all functions that load arguments 
# into runscripts. 

from DynamicETL_Dashboard.Utilities.Helpers import FillEnvironmentVariables, FillUniversalEnvironmentVariables, GetRegexPattern, IsNumeric, IsRegex, LoadJsonFile, StringIsDT
import json
import re
import os
import sys

############################
# DynamicETL_Validator.py
############################
def DynamicETLValidatorJsonArgs():
    """
    * Load all arguments for RunAPIs.py from
    DynamicETL_Validator.json.
    """
    argpath = 'ScriptArgs\\DynamicETL_Validator.json'
    req = set(['logpath'])
    # Ensure json file exists and can be loaded:
    if not os.path.exists(argpath):
        raise Exception('%s does not exist.' % argpath)
    try:
        args = json.load(open(argpath, 'rb'))
    except Exception as ex:
        raise Exception('Could not load %s. Reason: %s' % (argpath, str(ex)))
    missing = req - set(args)
    if missing:
        raise Exception('The following keys are missing from %s: %s' % (argpath, ','.join(missing)))
    
    ##################################
    # Required Arguments:
    ##################################
    # Ensure all required arguments are valid:
    errs = []
    # logpath:
    if not isinstance(args['logpath'], str):
        errs.append('logpath must be a string.')
    elif not args['logpath'].endswith('\\'):
        args['logpath'] += '\\'

    ##################################
    # Optional Arguments:
    ##################################
    # Ensure optional arguments are valid if used:
    if errs:
        raise Exception('\n'.join(errs))

    return args
