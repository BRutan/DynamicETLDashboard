#####################################
# Helpers.py
#####################################
# Description:
# * General helper functions for various classes. 

from datetime import datetime
import dateutil.parser as dateparse
from jsonargparse import ArgumentError
import os
import re

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

def StringIsDT(dateString, predicate = False):
    """
    * Return true if passed string is a date.
    Input:
    * dateString: String that could possibly represent a date.
    * predicate: If True then will return True/False if can or cannot convert.
    Otherwise an exception will be raised.
    Output:
    * Return true if passed date can be converted to a datetime object.
    """
    try:
        out = dateparse.parse(dateString)
        return (out if not predicate else True)
    except:
        if not predicate:
            raise Exception("%s is not a date/datetime string." % dateString)
        return False

################
# Decorators:
################
def CheckPath(path, nopath, isfile):
    if not nopath:
        if path and not isinstance(path, str):
            raise Exception('path must be a string or None.')
        elif isfile and '.' not in path:
            pass
