#####################################
# RegisterEndpoints.py
#####################################
# Description:
# * Register all endpoints to be
# used in application, ready to be
# fed into the FlaskAPIFactory.

from APIs.ETLSummaryReportAPI import ETLSummaryReporthealthcheck, GenerateReport
from APIs.MiscControllers import healthcheck

def register_endpoints():
    """
    * Register all endpoints to be
    used in application, ready to be
    fed into the FlaskAPIFactory.
    """
    endpoints = []
    endpoints.append({"func" : healthcheck, "name" : 'healthcheck', "route" : '/healthcheck', 'methods' : ['GET'] })
    # @etlsummaryreport_bp.route('/<databody>', methods = ['POST'])
    endpoints.append({"func" : ETLSummaryReporthealthcheck, "name" : 'etlsummaryreporthealthcheck', "route" : '/ETLSummaryReport/healthcheck', 'methods' : ['GET'], "inject" : True })
    endpoints.append({"func" : GenerateReport, "name" : 'GenerateReport', "route" : '/ETLSummaryReport/GenerateReport/', 'methods' : ['POST'], "inject" : True })

    return endpoints