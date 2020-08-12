#####################################
# ETLSummaryReportAPI.py
#####################################
# Description:
# * Generates
# ETLSummaryReportAPI reports when receives 
# data from DynamicEtl.Service or any application
# that generates ETLs and requests reports about
# their completion.
    
import copy
from Configs.ValidatorConfig import ValidatorConfig
from Configs.ETLSummaryReportConfig import ETLSummaryReportConfig
from DynamicETL_Dashboard.Logging.ScriptLogger import ScriptLogger
from flask_injector import FlaskInjector
from flask import Blueprint, Flask, request, Response
from injector import inject
from Reports.ETLSummaryReport import ETLSummaryReport

#controller_name = 'ETLSummaryReport'
#etlsummaryreport_bp = Blueprint('etlsummaryreport_bp', controller_name)

#################
# Endpoints:
#################
#@inject
#@etlsummaryreport_bp.route('/healthcheck', methods = ['GET'])
#def Healthcheck(self, log:ScriptLogger):
#    """
#    * Demonstrate that endpoint is running.
#    """
#    msg = "%s is running." % controller_name
#    log.Info(msg)
#    return Response(status = 200)

#@inject
#@etlsummaryreport_bp.route('/<databody>', methods = ['POST'])
#def GenerateReport(self, databody, config:ETLSummaryReportConfig, log:ScriptLogger):
#    """
#    * Generate report using ETL data.
#    Inputs:
#    * databody: json body. Must have appropriate
#    arguments.
#    """
#    # Attempt to generate report using passed POST body:
#    try:
#        errs = []
#        if not ETLSummaryReportAPI.IsValid(databody, errs):
#            raise ValueError('databody is invalid. Reason: %s' % ','.join(errs))
#        report = ETLSummaryReport(**databody)
#        report.GenerateReport(self.__config.ReportPath)
#        self.__log.Info('Finished generating ETLSummaryReport at %s.' % self.__config.ReportPath)
#        return Response(status = 200)
#    except Exception as ex:
#        self.__log.Error('Failed to generate ETLSummaryReport at %s. Reason: %s.' % (self.__config.reportpath, str(ex)))
#        return Response(status = 404)