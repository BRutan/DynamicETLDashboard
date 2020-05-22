#####################################
# Helpers.py
#####################################
# Description:
# * General helper functions for various classes. 

from datetime import datetime
import dateutil.parser as dateparse
import json
from jsonargparse import ArgumentError
import os
import re
from time import sleep
from tqdm import trange

def Countdown(numSeconds):
    """
    * Print countdown sequence
    for waiting.
    """
    if not isinstance(numSeconds, (int, float)):
        raise Exception('numSeconds must be numeric.')
    elif int(numSeconds) <= 0:
        raise Exception('numSeconds must be positive.')
    numSeconds = int(numSeconds)
    for i in trange(numSeconds):
        sleep(1)

def CheckPath(path, argname, exists = True, pathtype = None):
    errs = []
    if not isinstance(path, str):
        errs.append('path must be a string.')
    if pathtype and not pathtype(path):
        errs.append('path does not match path type.')
    if exists and not os.path.exists(path):
        errs.append('Entity at path does not exist.')
    elif not exists and os.path.exists(path):
        errs.append('Entity at path already exists.')
    if errs:
        raise ArgumentError(argname, '\n'.join(errs))
    return path

def CheckRegex(regStr, argname, regcheck = None):
    errs = []
    if not isinstance(regStr, str):
        errs.append('Argument must be a string.')
    if not regcheck is None and not re.match(regcheck, regStr):
        errs.append('Argument regex does not match pattern %s.' % str(regcheck))
    if errs:
        raise ArgumentError(argname, '\n'.join(errs))
    return regStr

def IsRegex(regStr):
    try:
        exp = re.compile(regStr)
    except:
        return False
    return True

def StringIsDT(dateString, returnval = False):
    """
    * Return true if passed string is a date.
    Input:
    * dateString: String that could possibly represent a date.
    * returnval: If True then will return converted date, otherwise returns True/False.
    Output:
    * Return true if passed date can be converted to a datetime object.
    """
    try:
        out = dateparse.parse(dateString)
        return (out if returnval else True)
    except:
        return False

def LoadJsonFile(jsonpath, config = None, configval = None):
    """
    * Load json file and optionally fill in environment variables using
    config dictionary with configval.
    """
    errs = []
    if not isinstance(jsonpath, str):
        errs.append('jsonpath must be a string.')
    else:
        jsonpath = jsonpath.strip()
        if not jsonpath.endswith('.json'):
            errs.append('jsonpath must point to json file.')
        elif not os.path.exists(jsonpath):
            errs.append('jsonpath does not exist.')
    if not config is None and not isinstance(config, dict):
        errs.append('config must be a dictionary.')
    if not configval is None and not isinstance(configval, str):
        errs.append('configval must be a string.')
    if errs:
        raise Exception('\n'.join(errs))
    jsonobj = json.load(open(jsonpath, 'rb'))
    jsonobj = TrimAll(jsonobj)
    if not config is None:
        jsonobj = FillEnvironmentVariables(jsonobj, config, configval)
    return jsonobj

def TrimAll(val):
    """
    * Trim all spaces from ends of each string in container.
    Inputs
    * val: iterable or string.
    """
    if isinstance(val, str):
        val = val.strip()
    elif isinstance(val, dict):
        for key in val:
            val[key] = TrimAll(val[key])
    elif isinstance(val, tuple):
        val = list(val)
        val = tuple(TrimAll(val))
    elif hasattr(val, '__iter__'):
        for num in range(0, len(val)):
            val[num] = TrimAll(val[num])
    return val

def FillEnvironmentVariables(target, configjson, configval):
    """
    * Fill all environment variable strings using 
    values in configjson dictionary, using configval.
    Inputs:
    * target: dictionary, list or string containing variables to fill.
    * configjson: dictionary containing environment variables.
    * configval: string containing configuration value to set environment variables.
    """
    errs = []
    if not isinstance(target, (str, list, dict)):
        errs.append('target must be one of (str, list, dict).')
    if not isinstance(configjson, dict):
        errs.append('configjson must be a dictionary.')
    if not isinstance(configval, str):
        errs.append('configval must be a string.')
    if errs:
        raise Exception('\n'.join(errs))
    
    if isinstance(target, str):
        for key in configjson:
            copy = key
            var = '{' + copy if not copy.startswith('}') else copy
            var = var + '}' if not var.endswith('}') else var
            rep = configjson[key][configval]
            target = target.replace(var, rep)
        target = FixPath(target)
    elif isinstance(target, dict):
        for key in target:
            target[key] = FillEnvironmentVariables(target[key], configjson, configval)
    else:
        for num in range(0, len(target)):
            target[num] = FillEnvironmentVariables(target[num], configjson, configval)

    return target

def FixPath(path):
    """
    * Fix too many backslash issue that occurs when reading
    file paths in json files.
    Inputs:
    * path: string to fix.
    """
    if isinstance(path, str):
        path = path.replace('\\\\', '\\')
    return path


################
# Decorators:
################
def CheckPath(path, nopath, isfile):
    if not nopath:
        if path and not isinstance(path, str):
            raise Exception('path must be a string or None.')
        elif isfile and '.' not in path:
            pass
