#####################################
# FileWatcherIssueParser.py
#####################################
# Description:
# * 

import os

class FileWatcherIssueParser:
    """
    * Parse issues that occur in FileWatcher log file.
    """
    def __init__(self, filewatcherlogfolder):
        """
        * Parse issues that occur in FileWatcher log file.
        Inputs:
        * filewatcherlogfolder: String to folder containing
        filewatcher logs.
        """
        FileWatcherIssueParser.__Validate(filewatcherlogfolder)



    ################
    # Private Helpers:
    ################
    @staticmethod
    def __Validate(filewatcherlogfolder):
        """
        * Validate constructor parameters.
        """
        if not isinstance(filewatcherlogfolder, str):
            raise Exception('filewatcherlogfolder must be a string.')
        elif not os.isdir(filewatcherlogfolder):
            raise Exception('filewatcherlogfolder does not point to valid folder.')
