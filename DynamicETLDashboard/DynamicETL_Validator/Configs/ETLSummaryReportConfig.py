#####################################
# ETLSummaryReportConfig.py
#####################################
# Description:
# * Config section for ETLSummaryReportAPI
# controller.

import os

class ETLSummaryReportConfig:
    """
    * Config section for ETLSummaryReportAPI
    controller.
    """
    __req = set(['reportpath'])
    def __init__(self, reportpath):
        """
        * Generate new object from section dictionary using
        baseurl.
        Inputs:
        * section: json dictionary section from config file.
        """
        # Normalize attributes:
        ETLSummaryReportConfig.__Validate(reportpath)
        self.__SetProperties(reportpath)

    ####################
    # Properties:
    ####################
    @property
    def ReportPath(self):
        return self.__reportpath

    ####################
    # Private Helpers:
    ####################
    @staticmethod
    def __Validate(reportpath):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(reportpath, str):
            errs.append('reportpath must be a string.')
        else:
            folder, file = os.path.split(reportpath)
            if folder and not os.path.exists(folder):
                errs.append('reportpath does not point to existing folder.')
            if not file.endswith('.xlsx'):
                errs.append('reportpath must point to an .xlsx file.')
        if errs:
            raise Exception('\n'.join(errs))

    def __SetProperties(self, reportpath):
        """
        * Set object properties from constructor parameters.
        """
        self.__reportpath = reportpath