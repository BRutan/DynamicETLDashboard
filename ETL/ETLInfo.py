#####################################
# ETLInfo.py
#####################################
# Description:
# * Aggregates all useful information about
# an ETL (tablename, server, file locations).

import json
import xlsxwriter
from Utilities.Helpers import FillEnvironmentVariables

class ETLInfo:
    """
    * Immutable object with aggregated information 
    regarding ETLs present in various appsettings files.
    """
    def __init__(self, etlname, configjson, etlfilepathsjson, filewatcherjson, servicejson):
        """
        * Collect all information regarding passed etl.
        Inputs:
        * etlname: String name of ETL want information about.
        * configjson: Dictionary containing json object loaded
        with "config.json" environment variables.
        * etlfilepathsjson: Dictionary containing filepaths listed
        in "etlfilepaths.json":
        * servicejson: Dictionary containing DynamicETL.Service Appsettings.json data.
        """
        ETLInfo.__Validate(etlname, configjson, etlfilepathsjson, filewatcherjson, servicejson)
        self.__etlname = etlname
        self.__CollectInfo(configjson, etlfilepathsjson, filewatcherjson, servicejson)

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
        * reportpath: Path to output report. Must point to .xlsx file.
        """
        if not isinstance(reportpath, str):
            raise Exception('reportpath must be a string.')
        elif not reportpath.endswith('.xlsx'):
            raise Exception('reportpath must point to .xlsx file.')
        wb = xlsxwriter.Workbook(reportpath)
        self.__WriteSummarySheet(wb)
        wb.close()

    ###################
    # Private Helpers:
    ###################
    def __WriteSummarySheet(self, wb):
        """
        * Write summary sheet for generated report.
        """
        sheet = wb.add_worksheet('%s Summary' % self.__etlname)


    def __CollectInfo(self, configjson, etlfilepathsjson, filewatcherjson, servicejson):
        """
        * Collect all information regarding etlname
        using appsettings files indicated in paths.
        """
        self.__inputfilesig = None
        self.__inputfolders = None
        self.__servername = None
        self.__tablename = None
        try:
            self.__CollectInfoETLPaths(etlfilepathsjson)
            self.__CollectInfoFileWatcher(filewatcherjson)
            self.__CollectInfoDynamicETLService(servicejson)
        except Exception as ex:
            pass


    def __CollectInfoETLPaths(self, etlpathsjson):
        """
        * Collect information from json object loaded with etlpaths.json.
        """
        # Loop through to find etl:
        target = None
        for config in etlpathsjson['files']:
            if config['subject'].lower() == self.__etlname.lower():
                target = config
                break
        if target is None:
            raise Exception('ETL %s not present in etlpaths.json.' % self.__etlname)

        

    def __CollectInfoFileWatcher(self, filewatcherjson):
        """
        * Collect useful info from FileWatcher appsettings file.
        """
        pass

    def __CollectInfoDynamicETLService(self, servicejson):
        """
        * Collect useful info from DynamicETL.Service appsettings file.
        """
        pass

    
    @staticmethod
    def __Validate(etlname, configjson, etlfilepathsjson, filewatcherjson, servicejson):
        """
        * Validate constructor arguments.
        """
        errs = []
        if not isinstance(etlname, str):
            errs.append('etlname must be a string.')
        if not isinstance(configjson, dict):
            errs.append('configjson must be a dictionary.')
        if not isinstance(etlfilepathsjson, dict):
            errs.append('etlfilepathsjson must be a dictionary.')
        if not isinstance(filewatcherjson, dict):
            errs.append('filewatcherjson must be a dictionary.')
        if not isinstance(servicejson, dict):
            errs.append('servicejson must be a dictionary.')
        if errs:
            raise Exception('\n'.join(errs))