##############################################################################
## NonFatal.py
##############################################################################
## Description:
## * Contains all non-fatal exceptions that will not trigger application
## close when raised, but will be output to log file and stdout.
## All non-fatal exceptions will be handled by ExceptionAggregator class, since the application 
## can have multiple non-fatal errors:

from abc import ABCMeta, abstractmethod
from datetime import datetime
from Exceptions.ETLDashboardExcept import ETLDashboardExcept as base
import Exceptions.ExceptionContainerType as Container
import DirectoryTypes.PathType as PathType
import sys
from sortedcontainers import SortedList
from Utilities.Helpers import StringIsDT

__all__ = [ 'FailedToGeneratePNGS', 'FailedToGeneratePDF', 'FailedToOutputPNGS', 'MerlinCurvesMissing', 'NoPlotsLoaded', 'OutlookFailed', 'NonFatal' ]

########################################################################################################
# Base Classes:
########################################################################################################
class NonFatal(base):
    " Abstract base class for non-fatal exceptions, that will be printed and output to log file on application exit."
    __metaclass__ = ABCMeta
    ##########################################################
    ## Constructors:
    ##########################################################  
    def __init__(self, callingFunc, specific = '', timestamp = datetime.now()):
        if sys.version_info[0] > 2:
            super().__init__(callingFunc, specific, timestamp)
        else:
            super(NonFatal, self).__init__(callingFunc, specific, timestamp)