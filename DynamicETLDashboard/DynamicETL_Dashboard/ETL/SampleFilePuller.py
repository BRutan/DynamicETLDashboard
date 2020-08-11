#####################################
# SampleFilePuller.py
#####################################
# Description:
# * Pull sample file for one or more 
# ETLs.

import re
import os
from Utilities.FileConverter import FileConverter

class SampleFilePuller:
    """
    * Class that pulls sample files
    at configured directories for one or more ETLs, and
    outputs to target folder.
    """
    def __init__(self, etlconfigs):
        """
        * Instantiate object and store
        all filepaths to 
        """
        SampleFilePuller.__Validate(etlconfigs)
        self.__etldatapaths = {}
        self.__etlConfigs = {}
        self.__GenerateRegex(etlconfigs)
    ##################
    # Properties:
    ##################
    def ETLDataPaths(self):
        return self.__etlpaths.copy()
    def ETLDataPathRegexes(self):
        return self.__etlpathregex.copy()
    ##################
    # Interface Methods:
    ##################
    def PullFiles(self, etl, outputFolder, maxNum):
        """
        * Pull sample files for one or more passed ETLs,
        output into target folder.
        Inputs:
        * etl: string name of etl or iterable of etl name strings. Must
        be configured in the FileWatcher appsettings file.
        * outputFolder: String folder to output all data files.
        * maxNum: Maximum number of data files to pull.
        """
        SampleFilePuller.__Validate(etl, outputFolder, maxNum)
        etls = [etl] if not hasattr(etl, '__iter__') else etl
        for etl in etls:
            paths = self.__etldatapaths[etl]

    ##################
    # Private Helpers:
    ##################
    def __GenerateRegex(etlconfigs):
        """
        * Generate { ETL -> FileSourcePath } all regular 
        expressions using configuration file.
        """
        self.__etlConfigs
        for elem in etlconfigs["files"]:
            etl = elem['subject']
            # Fill in paths for all ETLs based on location
            paths = elem['inbound']
            self.__etlConfigs[etl] = paths

    @staticmethod
    def __Validate(etl, configs, outputFolder, maxNum):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(etl, (str, list)):
            errs.append('etl must be an individual string ETL name or a list of string ETL names.')
        elif isinstance(etl, str):
            # Ensure etl is present in all configs:
            pass
        elif isinstance(etl, list):
            if any([True for etlname in etl if not isinstance(etlname, str)]):
                errs.append('etl must only contain strings if a list.')
            # Ensure all etls have been present in all configs:
            else:
                missing = [etlname for etlname in etl if not isinstance(etlname, str)]                
                if missing:
                    errs.append('The following etls are not configured in %s: %s' % ('abc', ','.join(missing)))
        if not isinstance(outputFolder, str):
            errs.append('outputFolder must be a string.')
        elif not os.path.isdir(outputFolder):
            errs.append('outputFolder does not point to valid directory.')
        if not isinstance(maxNum, (float, int)):
            errs.append('maxNum must be numeric.')
        elif not int(maxNum) > 0:
            errs.append('maxNum must be positive.')
        if errs:
            raise Exception('\n'.join(errs))