#####################################
# ETLSummaryReportAPI.py
#####################################
# Description:
# * Derived APIReceiver that generates
# ETLSummaryReportAPI reports when receives 
# data from DynamicEtl.Service or any application
# that generates ETLs and requests reports about
# their completion.

from ETLDashboard.Data.APIConfigReader import APIConfig
from ETLDashboard.Data.APIReceiver import APIReceiver

class ETLSummaryReportAPI(APIReceiver):
    """
    * Derived APIReceiver that generates
    ETLSummaryReportAPI reports when receives 
    data from DynamicEtl.Service or any application
    that generates ETLs and requests reports about
    their completion.
    """
    def __init__(self, config, log):
        """
        * Generate new API for generating ETLSummaryReports when
        provided data.
        """
        ETLSummaryReportAPI.__Validate(config)
        super().__init__(config.AppName, '')
        self.__SetProperties(config, log)

    #################
    # Endpoints:
    #################
    #@super().App.route('')
    def GenerateReport(self, body):
        """
        * Generate report using data.
        Inputs:
        * body: json body. Must have appropriate
        arguments.
        """
        if not ETLSummaryReportAPI.IsValid(body):
            raise ValueError('body is invalid.')

    #################
    # Interface Methods:
    #################
    @classmethod
    def IsValid(cls, body):
        """
        * Verify that json body is valid.
        Inputs:
        * body: Expecting a json dictionary.
        """
        errs = []
        if not isinstance(body, dict):
            errs.append('body must be a json dictionary.')
        if errs:
            raise Exception('\n'.join(errs))


    #################
    # Private Helpers:
    #################
    @staticmethod
    def __Validate(config):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(config, APIConfig):
            errs.append('config must be an APIConfig object.')
        if errs:
            raise Exception('\n'.join(errs))

