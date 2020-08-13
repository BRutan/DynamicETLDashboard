#####################################
# AppConfig.py
#####################################
# Description:
# * Object to store flask API configurations
# from .json file for generation at runtime.

import copy
import re

class AppConfig:
    """
    * Object to store flask API configurations
    from .json file for generation at runtime.
    """
    __req = set(['appname', 'hostname'])
    __urlPattern = 'http://.+'
    __urlPatternRE = re.compile(__urlPattern)
    def __init__(self, appname, hostname):
        """
        * Convert dictionary key and value into
        an APIConfig object.
        Inputs:
        * appname: Name of API.
        * hostname: Dictionary object containing all
        required configurations for API.
        """
        AppConfig.__Validate(appname, hostname)
        self.__SetProperties(appname, hostname)

    ####################
    # Properties:
    ####################
    @property
    def AppName(self):
        return self.__appname
    @property
    def Hostname(self):
        return self.__hostname

    ####################
    # Interface Methods:
    ####################
    @classmethod
    def RequiredAttrs(cls):
        return copy.deepcopy(AppConfig.__req)

    ####################
    # Private Helpers:
    ####################
    @staticmethod
    def __Validate(appname, hostname):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(appname, str):
            errs.append('appname must be a string.')
        if not isinstance(hostname, str):
            errs.append('hostname must be a string.')
        #elif not AppConfig.__urlPatternRE.match(hostname):
        #    errs.append('hostname must match pattern %s.' % AppConfig.__urlPattern)
        if errs:
            raise Exception('\n'.join(errs))

    def __SetProperties(self, appname, hostname):
        """
        * Get object properties from constructor parameters.
        """
        self.__appname = appname
        self.__hostname = hostname