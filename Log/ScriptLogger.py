#####################################
# ScriptLogger.py
#####################################
# Description:
# * Wrapper class to log events.

import logging

class ScriptLogger:
    """
    * Wrapper class to log events.
    """
    def __init__(self, outLocation):
        """
        * Instantiate logger to be output at
        outLocation.
        Inputs:
        * outLocation: Output location for log file.
        Must be a string.
        """
        ScriptLogger.__Validate(outLocation)
        self.__outlocation = outLocation
        logging.basicConfig(filename = outLocation, level = logging.DEBUG)

    #################
    # Properties:
    #################
    @property
    def OutputLocation(self):
        return self.__outlocation

    #################
    # Interface Methods:
    #################
    def Exception(self, msg):
        """
        * Print 'Exception' message in log file.
        Inputs:
        * msg: String message.
        """
        if not isinstance(msg, str):
            raise Exception('msg must be a string.')
        logging.exception(msg)

    def Error(self, msg):
        """
        * Print 'Error' message in log file.
        Inputs:
        * msg: String message.
        """
        if not isinstance(msg, str):
            raise Exception('msg must be a string.')
        logging.error(msg)

    def Critical(self, msg):
        """
        * Print 'Critical' message in log file.
        Inputs:
        * msg: String message.
        """
        if not isinstance(msg, str):
            raise Exception('msg must be a string.')
        logging.critical(msg)

    def Info(self, msg):
        """
        * Print 'Info' message in log file.
        Inputs:
        * msg: String message.
        """
        if not isinstance(msg, str):
            raise Exception('msg must be a string.')
        logging.info(msg)
    
    def Warning(self, msg):
        """
        * Print 'Warning' message in log file.
        Inputs:
        * msg: String message.
        """
        if not isinstance(msg, str):
            raise Exception('msg must be a string.')
        logging.warning(msg)

    #################
    # Private Helpers:
    #################
    @staticmethod
    def __Validate(outLocation):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(outLocation, str):
            errs.append('outLocation must be a string.')
        if errs:
            raise Exception('\n'.join(errs))
