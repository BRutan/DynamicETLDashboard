#####################################
# FileVaultIssueParser.py
#####################################
# Description:
# * Determine where filevault issues have
# occurred in DynamicETL.Service log file.

import csv
import os
import re
from Utilities.FileConverter import FileConverter

class FileVaultIssueParser:
    """
    * 
    """
    __logfileSig = 'DynamicEtl.Service.log'
    __dataDict = {'TimeStamp' : [], 'ETLName' : [], 'StackTrace' : []}
    def __init__(self, servicelogfolder):
        """
        * Determine where all issues occurred and
        generate csv file with timestamp, etl name and
        stacktrace where 500 error issue occurred.
        Inputs:
        * servicelogfolder: Path to DynamicETL.Service logfile.
        """
        FileVaultIssueParser.__Validate(servicelogfolder)
        self.__FindIssues(servicelogfolder)

    ##################
    # Interface Methods:
    ##################
    def GenerateFile(self, outpath):
        """
        * Generate file containing detailed info
        about filevault issues.
        """
        if not isinstance(outpath, str):
            raise Exception('outpath must be a string.')
        elif not outpath.endswith('.csv'):
            raise Exception('outpath must point to csv file.')
        



    ##################
    # Private Helpers:
    ##################
    def __FindIssues(self, servicelogfolder):
        """
        * Find all 500 issues that occurred.
        """
        # Find matching files:
        data = FileVaultIssueParser.__dataDict
        files = FileConverter.GetAllFilePaths(servicelogfolder, FileVaultIssueParser.__logfileSig)
        stackTraceIndic = 'System.Net.WebException: The remote server returned an error: (500) Internal Server Error.'
        # {'TimeStamp' : [], 'ETLName' : [], 'StackTrace' : []}
        for file in files:
            with open(file, 'r') as f:
                lines = [line for line in f]
                for num, line in enumerate(lines):
                    if '(500) Internal Server Error' in line:
                        # Get the stack trace:
                        stackTrace = ''
                        row = num + 1
                        while stackTraceIndic not in stackTrace:
                            stackTrace = lines[row]
                            row += 1
                        # Find the corresponding ETL:
                        row = num
                        
                
    @staticmethod
    def __Validate(servicelogfolder):
        """
        * Validate constructor parameters.
        """
        if not isinstance(servicelogfolder, str):
            raise Exception('servicelogfolder must be a string.')
        elif not os.path.isdir(servicelogfolder):
            raise Exception('servicelogfolder must point to a valid folder.')