#####################################
# DynamicETLIssueParser.py
#####################################
# Description:
# * Determine where DynamicETL.Service issues have
# occurred in DynamicETL.Service log file.

import csv
import os
from pandas import DataFrame
import re
from Utilities.FileConverter import FileConverter

class DynamicETLIssueParser:
    """
    * Summarize issues that occur with ETLs.
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
        DynamicETLIssueParser.__Validate(servicelogfolder)
        self.__FindIssues(servicelogfolder)

    ##################
    # Interface Methods:
    ##################
    def GenerateFile(self, outpath):
        """
        * Generate file containing detailed info
        about DynamicETL.Service issues.
        Input:
        * outpath: String pointing to csv file.
        """
        if not isinstance(outpath, str):
            raise Exception('outpath must be a string.')
        elif not outpath.endswith('.csv'):
            raise Exception('outpath must point to csv file.')
        self.__data.to_csv(outpath)

    ##################
    # Private Helpers:
    ##################
    def __FindIssues(self, servicelogfolder):
        """
        * Find all 500 issues that occurred.
        """
        # Find matching files:
        data = DynamicETLIssueParser.__dataDict
        files = FileConverter.GetAllFilePaths(servicelogfolder, DynamicETLIssueParser.__logfileSig)
        stackTraceIndic = 'System.Net.WebException: The remote server returned an error: (500) Internal Server Error.'
        timestampRE = re.compile('\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
        for file in files:
            with open(files[file], 'r') as f:
                lines = [line for line in f]
                for num, line in enumerate(lines):
                    if '(500) Internal Server Error' in line:
                        # Get the stack trace:
                        stackTrace = ''
                        row = num + 1
                        stackTrace = lines[row]
                        while stackTraceIndic not in stackTrace:
                            stackTrace = lines[row]
                            row += 1
                        # Find the corresponding ETL:
                        row = num - 1
                        etlname = lines[row]
                        while 'ETL' not in etlname:
                            etlname = lines[row]
                            row -= 1
                        data['StackTrace'] = stackTrace
                        data['ETLName'] = etlname
                        data['TimeStamp'] = timestampRE.search(line)
        self.__data = DataFrame(data)
               
    @staticmethod
    def __Validate(servicelogfolder):
        """
        * Validate constructor parameters.
        """
        if not isinstance(servicelogfolder, str):
            raise Exception('servicelogfolder must be a string.')
        elif not os.path.isdir(servicelogfolder):
            raise Exception('servicelogfolder must point to a valid folder.')