#####################################
# ETLInfo.py
#####################################
# Description:
# * Aggregates all useful information about
# an ETL (tablename, server, file locations).

import csv
from Helpers import FillEnvironmentVariables
import json

class ETLInfo:
    """
    * Immutable object with aggregated information 
    regarding ETLs present in various appsettings files.
    """
    def __init__(self, etlname, jsonpaths):
        """
        * Collect all information regarding passed etl.
        Inputs:
        * etlname: String name of ETL want information about.
        * jsonpaths: Dictionary with following keys:
        - 
        """
        ETLInfo.__Validate(etlname, jsonpaths)
        self.__etlname = etlname
        self.__CollectInfo(etlname, jsonpaths)

    ###################
    # Properties:
    ###################
    @property
    def ETLName(self):
        return self.__etlname
    @property
    def InputFileSignature(self):
        return self.__inputfilesig
    @property
    def InputFolders(self):
        return self.__inputfolders
    @property
    def ServerName(self):
        return self.__servername
    @property
    def TableName(self):
        return self.__tablename
    ###################
    # Interface Methods:
    ###################
    def Summarize(self):
        """
        * Return string summarizing ETL for display purposes.
        """
        out = ['-' * 20, self.__etlname, '-' * 20]
        out.append('ServerName: %s' % self.__servername)
        out.append('TableName: %s' % self.__tablename)

        return '\n'.join(errs)

    def GenerateSummaryReport(self, reportpath):
        """
        * Generate report at path.
        Inputs:
        * reportpath: Path to output report. Must point to .csv file.
        """
        if not isinstance(reportpath, str):
            raise Exception('reportpath must be a string.')
        elif not reportpath.endswith('.csv'):
            raise Exception('reportpath must point to .csv file.')
        lines = []
        with open(reportpath, 'w', newline = '') as f:
            writer = csv.writer(f)

    ###################
    # Private Helpers:
    ###################
    def __CollectInfo(self, jsonpaths):
        """
        * Collect all information regarding etlname
        using appsettings files indicated in paths.
        """
        self.__inputfilesig = None
        self.__inputfolders = None
        self.__servername = None
        self.__tablename = None
        errs = []
        errs.extend(self.__CollectInfoFileWatcher(jsonpaths['filewatcherappsettingstemplatepath']))
        errs.extend(self.__CollectInfoDynamicETLService(jsonpaths['dynamicetlservicepath']))
        
        if errs:
            raise Exception('\n'.join(errs))

    def __CollectInfoFileWatcher(self, appsettingspath):
        """
        * Collect useful info from FileWatcher appsettings file.
        """
        errs = []
        try:
            serviceAppsettings = json.load(open(appsettingspath, 'rb'))
        except Exception as ex:
            errs.append('Could not read FileWatcher appsettings-template.json.')
            errs.append('Reason: %s' % str(ex))
            return errs

        return errs

    def __CollectInfoDynamicETLService(self, appsettingspath):
        """
        * Collect useful info from DynamicETL.Service appsettings file.
        """
        errs = []
        try:
            serviceAppsettings = json.load(open(appsettingspath, 'rb'))
        except Exception as ex:
            errs.append('Could not read Service appsettings.json.')
            errs.append('Reason: %s' % str(ex))
            return errs

        return errs

    @staticmethod
    def __Validate(etlname, jsonpaths):
        """
        * Validate constructor arguments.
        """
        errs = []
        if not isinstance(etlname, str):
            errs.append('etlname must be a string.')
        if not isinstance(jsonpaths, dict):
            errs.append('jsonpaths must be a dictionary.')
        else:
            req = set(['filewatcherappsettingstemplatepath', 'dynamicetlservicepath'])
            missing = req - set(jsonpaths)
            if missing:
                errs.append('The following required keys missing from args: %s' % ','.join(missing))
        if errs:
            raise Exception('\n'.join(errs))