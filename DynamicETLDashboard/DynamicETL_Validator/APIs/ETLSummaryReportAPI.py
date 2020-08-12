#####################################
# ETLSummaryReportAPI.py
#####################################
# Description:
# * Derived APIReceiver that generates
# ETLSummaryReportAPI reports when receives 
# data from DynamicEtl.Service or any application
# that generates ETLs and requests reports about
# their completion.

import copy
from APIs.APIReceiver import APIReceiver
from DependencyInjector.DI import inject_api_dependencies
from Configs.ValidatorConfig import ValidatorConfig
from Configs.ETLSummaryReportConfig import ETLSummaryReportConfig
from DynamicETL_Dashboard.Logging.ScriptLogger import ScriptLogger
from flask_injector import FlaskInjector
from flask import Blueprint, Flask, request
from Reports.ETLSummaryReport import ETLSummaryReport

prefix = 'ETLSummaryReportAPI'
etlsummaryreport_bp = Blueprint('etlsummaryreport_bp', "ETLSummaryReportAPI")

class ETLSummaryReportAPI(APIReceiver):
    """
    * Derived APIReceiver that generates
    ETLSummaryReportAPI reports when receives 
    data from DynamicEtl.Service or any application
    that generates ETLs and requests reports about
    their completion.
    """
    __req = copy.deepcopy(ETLSummaryReport.RequiredArgs)
    @inject
    def __init__(self, config:ETLSummaryReportConfig, log:ScriptLogger):
        """
        * Generate new API for generating ETLSummaryReports when
        provided data.
        """
        ETLSummaryReportAPI.__Validate(config, log)
        super().__init__("ETLSummaryReportAPI")
        self.__SetProperties(config, log)

    #################
    # Endpoints:
    #################
    @etlsummaryreport_bp.route('/GenerateReport/<databody>', methods = ['POST'])
    def GenerateReport(self, databody):
        """
        * Generate report using ETL data.
        Inputs:
        * databody: json body. Must have appropriate
        arguments.
        """
        # Attempt to generate report using passed POST body:
        try:
            errs = []
            if not ETLSummaryReportAPI.IsValid(databody, errs):
                raise ValueError('databody is invalid. Reason: %s' % ','.join(errs))
            report = ETLSummaryReport(**databody)
            report.GenerateReport(self.__config.reportpath)
            self.__log.Info('Finished generating ETLSummaryReport at %s.' % self.__config.reportpath)
        except Exception as ex:
            self.__log.Error('Failed to generate ETLSummaryReport at %s. Reason: %s.' % (self.__config.reportpath, str(ex)))

    #################
    # Interface Methods:
    #################
    @classmethod
    def IsValid(cls, body, errs):
        """
        * Verify that json body is valid.
        Inputs:
        * body: Expecting a json dictionary.
        """
        errs = []
        if not isinstance(body, dict):
            errs.append('body must be a json dictionary.')
        else:
            missing = ETLSummaryReport.__req - set(body)
            if missing:
                errs.append('The following required attributes are missing from body: %s.' % ','.join(missing))
        
        return False if errs else True

    #################
    # Private Helpers:
    #################
    @staticmethod
    def __Validate(config, log):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(config, ETLSummaryReportConfig):
            errs.append('config must be an ETLSummaryReportConfig object.')
        if not isinstance(log, ScriptLogger):
            errs.append('log must be a ScriptLogger object.')
        if errs:
            raise Exception('\n'.join(errs))

    def __SetProperties(self, config, log):
        """
        * Set properties from constructor parameters.
        """
        self.__config = config
        self.__log = log