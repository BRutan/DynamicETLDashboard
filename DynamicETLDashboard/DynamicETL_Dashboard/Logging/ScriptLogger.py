#####################################
# ScriptLogger.py
#####################################
# Description:
# * Wrapper class to log events.

from datetime import datetime
import logging
import os
import re

class ScriptLogger:
    """
    * Wrapper class to log events.
    """
    __fileRE = re.compile('$\.[a-z1-9]+', re.IGNORECASE)
    def __init__(self, outFolder, appName):
        """
        * Instantiate logger to be output at
        outFolder.
        Inputs:
        * outFolder: String output location for log file, without
        file name.
        * appName: String application name.
        Notes:
        * Logfile will be 
        """
        ScriptLogger.__Validate(outFolder, appName)
        self.__SetProperties(outFolder, appName)

    #################
    # Properties:
    #################
    @property
    def ApplicationName(self):
        return self.__appname
    @property
    def CreateDate(self):
        return self.__createdate
    @property
    def CreateDateStr(self):
        return self.__createdate.strftime('%Y_%m_%d')
    @property
    def FullPath(self):
        return self.__fullpath
    @property
    def OutputFolder(self):
        return self.__outfolder
    
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
    def __SetProperties(self, outFolder, appName):
        """
        * Set object properties from constructor parameters.
        """
        self.__outfolder = outFolder if outFolder.endswith('\\') else outFolder + '\\'
        self.__logger = logging.getLogger(appName)
        self.__logger.setLevel(logging.DEBUG)
        self.__createdate = datetime.today()
        self.__fullpath = '%s%s_%s.log' % (self.__outfolder, appName, self.CreateDateStr) 
        self.__handler = logging.FileHandler(self.__fullpath)
        self.__handler.setLevel(logging.DEBUG)
        self.__appname = appName
        self.__handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.__logger.addHandler(self.__handler)

    @staticmethod
    def __Validate(outFolder, appName):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(outFolder, str):
            errs.append('outFolder must be a string.')
        elif ScriptLogger.__fileRE.match(outFolder):
            errs.append('outFolder must not point to a file.')
        elif outFolder and not os.path.exists(outFolder):
            errs.append('outFolder does not exist.')
        if not isinstance(appName, str):
            errs.append('appName must be a string.')
        if errs:
            raise Exception('\n'.join(errs))
