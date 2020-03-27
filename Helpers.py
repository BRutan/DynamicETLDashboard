#####################################
# Helpers.py
#####################################
# Description:
# * 

import re
from datetime import datetime


def IsRegex(regStr):
    try:
        exp = re.compile(regStr)
    except:
        return False
    return True

def IsDatetimeFormatExp(expStr):
    return True

################
# Decorators:
################
def CheckPath(path, nopath, isfile):
    if not nopath:
        if path and not isinstance(path, str):
            raise Exception('path must be a string or None.')
        elif isfile and '.' not in path:
            pass
