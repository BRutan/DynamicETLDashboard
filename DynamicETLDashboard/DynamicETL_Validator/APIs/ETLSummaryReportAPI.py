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
from flask import Blueprint, Flask, request, Response, make_response
from flask_injector import FlaskInjector
from injector import ClassProvider, InstanceProvider, inject, Injector, SingletonScope
from Reports.ETLSummaryReport import ETLSummaryReport

#################
# Endpoints:
#################
def ETLSummaryReporthealthcheck():
    """
    * Demonstrate that endpoint is running.
    """
    return "ETLSummaryReport API is running."

@inject
def GenerateReport(log : ScriptLogger, config : ETLSummaryReportConfig):
    """
    * Generate report using ETL information sent using 
    json with POST REST request.
    """
    # Get the json object used in POST request, ensure validity:
    errs = []
    try:
        databody = request.get_json()
        databody = { key.lower() : databody[key] for key in databody }
        if not ETLSummaryReport.ArgsAreValid(databody, errs):
            log.Error('The following POST body issues occurred:')
            for err in errs:
                log.Error(err)
            raise Exception('POST body is invalid.')
    except Exception as ex:
        log.Error('Failed to parse json body for ETLSummaryReport. Reason: %s.' % str(ex))
        return Response(status = 400)
    # Attempt to generate report using passed POST body:
    report = None
    try:
        report = ETLSummaryReport(**databody)
        log.Info('Generating ETLSummaryReport for ETL %s at %s.' % (report.ETLName, config.ReportPath))
        report.GenerateReport(config.ReportPath)
        log.Info('ETLSummaryReport for %s finished with status Success.' % report.ETLName)
        return Response(status = 200)
    except Exception as ex:
        if report:
            msg = 'ETLSummaryReport for %s finished with status Fail.' % report.ETLName
        else:
            msg = 'ETLSummaryReport finished with status Fail.'
        log.Error(msg)
        return Response(status = 404)