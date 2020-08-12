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
    req = set(['apijson', 'debug', 'logpath'])
    # Ensure json file exists and can be loaded:
    if not os.path.exists(argpath):
        raise Exception('%s does not exist.' % argpath)
    try:
        args = json.load(open(argpath, 'rb'))
        # Normalize by making all keys lower case:
        args = { key.lower() : args[key] for key in args }
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

    # apijson:
    if not isinstance(args['apijson'], str):
        errs.append('apijson must be a string.')
    elif not args['apijson'].endswith('.json'):
        errs.append('apijson must point to a json file.')
    else:
        folder, file = os.path.split(args['apijson'])
        if folder and not os.path.exists(folder):
            errs.append('apijson containing folder for json file does not exist.')

    # appname:
    #if not isinstance(args['appname'], str):
    #    errs.append('appname must be a string.')

    # debug:
    if not isinstance(args['debug'], bool):
        errs.append('debug must be a boolean.')
    
    # logpath:
    if not isinstance(args['logpath'], str):
        errs.append('logpath must be a string.')
    else: 
        folder, file = os.path.split(args['logpath'])
        # Use just folder name since logger uses standardized format:
        if file:
            args['logpath'] = folder
        if args['logpath'] and not os.path.exists(args['logpath']):
            errs.append('logpath points to non-existent folder.')
        elif args['logpath'] and not args['logpath'].endswith('\\'):
            args['logpath'] += '\\'

    # websitename:
    #if not isinstance(args['websitename'], str):
    #    errs.append('websitename must be a string.')

    ##################################
    # Optional Arguments:
    ##################################
    # Ensure optional arguments are valid if used:
    if errs:
        raise Exception('\n'.join(errs))

    return args
