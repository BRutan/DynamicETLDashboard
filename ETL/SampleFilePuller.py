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
    * Singleton class that pulls sample files
    at configured directories for one or more ETLs, and
    outputs to target folder.
    """
    ##################
    # Interface Methods`:
    ##################
    @classmethod
    def PullFiles(etl, configs, outputFolder, maxNum):
        """
        * Pull sample files for one or more passed ETLs,
        output into target folder.
        """
        SampleFilePuller.__Validate(etl, configs, outputFolder, maxNum)
        etl = [etl] if isinstance(etl, str) else etl
        for etlname in etl:
            pass

    ##################
    # Private Helpers:
    ##################
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