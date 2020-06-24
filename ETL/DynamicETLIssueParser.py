#####################################
# DynamicETLIssueParser.py
#####################################
# Description:
# * Determine where DynamicETL.Service issues have
# occurred with particular etls in log file.

import csv
from datetime import datetime
import dateutil.parser as dtparser
import os
from pandas import DataFrame
import re
from Utilities.FileConverter import FileConverter

class DynamicETLIssueParser:
    """
    * Summarize issues that occur with ETLs.
    """
    __logfileSig = 'DynamicEtl.Service.log'
    __dataDict = {'TimeStamp' : [], 'ETLName' : [], 'FileId' : [], 'ErrorMessage' : [], 'StackTrace' : []}
    __timestampRE = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
    __etlnameRE = re.compile("['`].+['`]")
    __keymatchRE = re.compile('\[\d+\]')
    __fileIDRE = re.compile("{fileid:'.+'}")
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
        * Generate file containing detailed info about 
        DynamicETL.Service issues.
        Input:
        * outpath: String pointing to csv file.
        """
        if not isinstance(outpath, str):
            raise Exception('outpath must be a string.')
        elif not outpath.endswith('.csv'):
            raise Exception('outpath must point to csv file.')
        self.__data.to_csv(outpath, index = False)

    def ETLHasIssues(self, etlname, timerange = None):
        """
        * Determine if ETL experienced issues, optionally in some
        time frame. 
        If true, then return a summary string, otherwise return None.
        Inputs:
        * etlname: String name of ETL.
        Optional:
        * timerange: Tuple containing two datetimes,
        (LowerBound, UpperBound) to see if issues occurred
        within some interval.
        """
        errs = []
        if not isinstance(etlname, str):
            errs.append('etlname must be a string.')
        elif not timerange is None:
            if not isinstance(timerange, tuple):
                errs.append('timerange must be a tuple if provided.')
            elif any([not isinstance(elem, datetime) for elem in timerange]):
                errs.append('timerange must only contain datetime objects.')
            elif timerange[0] > timerange[1]:
                # Swap time interval if out of order:
                copy = timerange[0]
                timerange[0] = timerange[1]
                timerange[1] = copy
        if errs:
            raise Exception('\n'.join(errs))
        # Search for particular issues:
        data = self.__data.loc[self.__data['ETLName'].str.lower() == etlname.lower()]
        if not timerange is None:
            data = data.loc[data['TimeStamp'] >= timerange[0] and data['TimeStamp'] <= timerange[1]]
        if len(data) > 0:
            lines = ['The following issues occurred for %s:' % etlname]
            for row in range(0, len(data)):
                lines.append(data.iloc[row]['ErrorMessage'])
            return '\n'.join(lines)
        else:
            return None

    ##################
    # Private Helpers:
    ##################
    def __FindIssues(self, servicelogfolder):
        """
        * Find all etl issues that occurred.
        """
        # Find matching files:
        self.__data = DynamicETLIssueParser.__dataDict
        files = FileConverter.GetAllFilePaths(servicelogfolder, DynamicETLIssueParser.__logfileSig)
        for file in files:
            with open(files[file], 'r') as f:
                groups = DynamicETLIssueParser.__GroupAllJobs(f)
                for jobkey in groups:
                    self.__DetermineIssues(jobkey, groups[jobkey])
        self.__data = DataFrame(self.__data).sort_values('TimeStamp', ascending = False)

    @staticmethod
    def __GroupAllJobs(file):
        """
        * Group all issues by job key.
        """
        groups = {}
        prevGroup = None
        for line in file:
            if DynamicETLIssueParser.__keymatchRE.search(line):
                jobKey = DynamicETLIssueParser.__keymatchRE.search(line)[0]
                if jobKey not in groups:
                    groups[jobKey] = []
                else:
                    groups[jobKey].append(line)
                prevGroup = jobKey
            else:
                groups[prevGroup].append(line)
        return groups
    
    def __DetermineIssues(self, jobKey, grouplines):
        """
        * Determine issues for each job.
        """
        lineNum = 0
        msgsearchstop = ['INFO', 'ERROR']
        while lineNum < len(grouplines):
            line = grouplines[lineNum]
            if 'Etl finished with status Error' in line:
                etlname = DynamicETLIssueParser.__etlnameRE.search(line)[0].strip("'`")
                row = lineNum - 1
                errorMessage = grouplines[row]
                stackTrace = []
                while (not any([val in errorMessage for val in msgsearchstop]) or 'Email' in errorMessage) and row >= 0:
                    if 'at' in errorMessage:
                        stackTrace.append(errorMessage)
                    row -= 1
                    errorMessage = grouplines[row]
                # Get the fileid used in the etl:
                row = lineNum
                fileidsearchstop = 'Finished Etl for `%s` with arg' % etlname
                line = grouplines[row]
                while not fileidsearchstop in line and row <= len(grouplines):
                    row += 1
                    line = grouplines[row]
                matches = DynamicETLIssueParser.__fileIDRE.search(line)
                if matches:
                    fileid = matches[0]
                    fileid = re.search("'.+'", fileid)[0].strip("'")
                else:
                    fileid = ''
                self.__data['ETLName'].append(etlname)
                self.__data['FileId'].append(fileid)
                self.__data['ErrorMessage'].append(errorMessage.strip('\n'))
                self.__data['StackTrace'].append(''.join(stackTrace))
                self.__data['TimeStamp'].append(dtparser.parse(DynamicETLIssueParser.__timestampRE.search(errorMessage)[0]))
            lineNum += 1
               
    @staticmethod
    def __Validate(servicelogfolder):
        """
        * Validate constructor parameters.
        """
        if not isinstance(servicelogfolder, str):
            raise Exception('servicelogfolder must be a string.')
        elif not os.path.isdir(servicelogfolder):
            raise Exception('servicelogfolder must point to a valid folder.')