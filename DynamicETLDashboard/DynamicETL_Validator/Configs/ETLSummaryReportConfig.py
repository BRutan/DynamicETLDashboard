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
    def __init__(self, section):
        """
        * Generate new object from section dictionary using
        baseurl.
        Inputs:
        * section: json dictionary section from config file.
        """
        # Normalize attributes:
        ETLSummaryReportConfig.__Validate(section)
        self.__SetProperties(section)

    ####################
    # Properties:
    ####################
    @property
    def Reportpath(self):
        return self.__reportpath

    ####################
    # Private Helpers:
    ####################
    @staticmethod
    def __Validate(section):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(section, dict):
            errs.append('section must be a dictionary.')
        else:
            section = { sec.lower() : section[sec] for sec in section }
            missing = ETLSummaryReportConfig.__req - set(section)
            if missing:
                errs.append('section is missing the following attributes: %s.' % ','.join(missing))
            if 'reportpath' in section:
                if not isinstance(section['reportpath'], str):
                    errs.append('section::reportpath must be a string.')
                else:
                    folder, file = os.split(section['reportpath'])
                    if not os.path.exists(folder):
                        errs.append('section::reportpath does not point to existing folder.')
                    if not file.endswith('.xlsx'):
                        errs.append('section::reportpath must point to an .xlsx file.')
        if errs:
            raise Exception('\n'.join(errs))

    def __SetProperties(self, section):
        """
        * Set object properties from constructor parameters.
        """
        self.__reportpath = section["reportpath"]