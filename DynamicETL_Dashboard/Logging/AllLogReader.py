#####################################
# AllLogReader.py
#####################################
# Description:
# * Summarize issues that occur in all notable
# log files.

from Logging.DynamicETLServiceIssueParser import DynamicETLServiceIssueParser
from Logging.DynamicETLWebAPIIssueParser import DynamicETLWebAPIIssueParser
from Logging.FileWatcherIssueParser import FileWatcherIssueParser
import re
import os

class AllLogReader:
    """
    * Summarize issues that occur in all notable
    log files.
    """
    __reqArgs = set(['servicelogfolder', 'webapilogfolder', 'filewatcherlogfolder'])
    def __init__(self, **kwargs):
        """
        * Summarize issues that occur in all log files.
        """
        kwargs = {key.lower() : kwargs[key] for key in kwargs}
        AllLogReader.__Validate(kwargs)
        self.__servicereader = DynamicETLServiceIssueParser(kwargs['servicelogfolder'])
        self.__webapireader = DynamicETLWebAPIIssueParser(kwargs['webapilogfolder'])
        self.__filewatcherreader = FileWatcherIssueParser(kwargs['filewatcherlogfolder'])

    #################
    # Interface Methods:
    #################
    def GenerateIssueReport(self):
        pass

    def SummarizeETLIssues(self):
        pass
    
    #################
    # Private Helpers:
    #################
    @staticmethod
    def __Validate(**kwargs):
        """
        * Validate constructor arguments.
        """
        args = set(kwargs.keys())
        missing = AllLogReader.__reqArgs - args
        target = AllLogReader.__reqArgs.intersection(args)
        errs = []
        if missing:
            errs.append('The following required keys are missing: %s' % ', '.join(missing))
        if target:
            for arg in target:
                if not isinstance(target[arg], str):
                    errs.append('%s must be a string.' % arg)
                elif not os.path.isdir(target[arg]):
                    errs.append('%s does not point to valid folder.' % arg)
        if errs:
            raise Exception('\n'.join(errs))