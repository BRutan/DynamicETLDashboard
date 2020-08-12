#####################################
# ETLSummaryReportAPI.py
#####################################
# Description:
# * Generates ETLSummaryReportAPI reports when receives 
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

#################
# Endpoints:
#################
@inject
def ETLSummaryReporthealthcheck(self, log:ScriptLogger):
    """
    * Demonstrate that endpoint is running.
    """
    msg = "%s is running." % controller_name
    log.Info(msg)
    return Response(status = 200)

@inject
def GenerateReport(self, databody, config:ETLSummaryReportConfig, log:ScriptLogger):
    """
    * Generate report using ETL data.
    Inputs:
    * databody: json body. Must have appropriate
    arguments.
    """
    # Attempt to generate report using passed POST body:
    try:
        errs = []
        #if not ETLSummaryReportAPI.IsValid(databody, errs):
        #    raise ValueError('databody is invalid. Reason: %s' % ','.join(errs))
        report = ETLSummaryReport(**databody)
        report.GenerateReport(config.ReportPath)
        log.Info('Finished generating ETLSummaryReport at %s.' % config.ReportPath)
        return Response(status = 200)
    except Exception as ex:
        log.Error('Failed to generate ETLSummaryReport at %s. Reason: %s.' % (config.reportpath, str(ex)))
        return Response(status = 404)

def IsValidBody(self, databody):
    """
    * Indicate whether json body has proper parameters.
    """
    pass
