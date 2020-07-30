#####################################
# ScriptLogger.py
#####################################
# Description:
# * Wrapper class to log events.

import logging
import os

class ScriptLogger:
    """
    * Wrapper class to log events.
    """
    def __init__(self, outLocation, appName):
        """
        * Instantiate logger to be output at
        outLocation.
        Inputs:
        * outLocation: String output location for log file.
        * appName: String application name.
        """
        ScriptLogger.__Validate(outLocation, appName)
        self.__outlocation = outLocation
        self.__logger = logging.getLogger(appName)
        self.__logger.setLevel(logging.DEBUG)
        self.__handler = logging.FileHandler(outLocation)
        self.__handler.setLevel(logging.DEBUG)
        self.__appname = appName
        self.__handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.__logger.addHandler(self.__handler)

    #################
    # Properties:
    #################
    @property
    def ApplicationName(self):
        return self.__appname
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
        self.__logger.exception(msg)

    def Error(self, msg):
        """
        * Print 'Error' message in log file.
        Inputs:
        * msg: String message.
        """
        if not isinstance(msg, str):
            raise Exception('msg must be a string.')
        self.__logger.error(msg)

    def Critical(self, msg):
        """
        * Print 'Critical' message in log file.
        Inputs:
        * msg: String message.
        """
        if not isinstance(msg, str):
            raise Exception('msg must be a string.')
        self.__logger.critical(msg)

    def Info(self, msg):
        """
        * Print 'Info' message in log file.
        Inputs:
        * msg: String message.
        """
        if not isinstance(msg, str):
            raise Exception('msg must be a string.')
        self.__logger.info(msg)
    
    def Warning(self, msg):
        """
        * Print 'Warning' message in log file.
        Inputs:
        * msg: String message.
        """
        if not isinstance(msg, str):
            raise Exception('msg must be a string.')
        self.__logger.warning(msg)

    #################
    # Private Helpers:
    #################
    @staticmethod
    def __Validate(outLocation, appName):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(outLocation, str):
            errs.append('outLocation must be a string.')
        elif not os.path.exists(os.path.split(outLocation)[0]):
            errs.append('Containing folder for outLocation does not exist.')
        if not isinstance(appName, str):
            errs.append('appName must be a string.')
        if errs:
            raise Exception('\n'.join(errs))
