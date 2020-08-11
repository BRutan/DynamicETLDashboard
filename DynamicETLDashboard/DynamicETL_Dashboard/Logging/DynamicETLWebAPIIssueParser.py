#####################################
# DynamicETLWebAPIIssueParser.py
#####################################
# Description:
# * Summarize issues that occur in DynamicETL.WebApi.

import os

class DynamicETLWebAPIIssueParser:
    """
    * Summarize issues that occur in DynamicETL.WebApi
    """
    def __init__(self, webapilogpfolder):
        """
        * Parse issues in DynamicETL.WebAPI logfile.
        Inputs:
        * webapilogpfolder: String path to folder containing webapi logs.
        """
        DynamicETLWebAPIIssueParser.__Validate(webapilogpfolder)
        self.__folder = webapilogpfolder





    #################
    # Private Helpers:
    #################
    @staticmethod
    def __Validate(webapilogpfolder):
        """
        * Validate constructor arguments.
        """
        if not isinstance(webapilogpfolder, str):
            raise Exception('webapilogpfolder must be a string.')
        elif not os.isdir(webapilogpfolder):
            raise Exception('webapilogpfolder does not point to valid folder.')