#####################################
# ControllerConfig.py
#####################################
# Description:
# * Object to store flask API configurations
# from .json file for generation at runtime.

import copy
import re

class ControllerConfig:
    """
    * Object to store flask API configurations
    from .json file for generation at runtime.
    """
    __req = set(['appname', 'baseurl'])
    __urlPattern = 'http://.+'
    __urlPatternRE = re.compile(__urlPattern)
    def __init__(self, appname, baseurl):
        """
        * Convert dictionary key and value into
        an APIConfig object.
        Inputs:
        * appname: Name of API.
        * baseurl: Dictionary object containing all
        required configurations for API.
        """
        ControllerConfig.__Validate(appname, baseurl)
        self.__SetProperties(appname, baseurl)

    ####################
    # Properties:
    ####################
    @property
    def AppName(self):
        return self.__appname
    @property
    def BaseURL(self):
        return self.__baseurl

    ####################
    # Interface Methods:
    ####################
    @classmethod
    def RequiredAttrs(cls):
        return copy.deepcopy(ControllerConfig.__req)

    ####################
    # Private Helpers:
    ####################
    @staticmethod
    def __Validate(appname, baseurl):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(appname, str):
            errs.append('appname must be a string.')
        if not isinstance(baseurl, str):
            errs.append('baseurl must be a string.')
        elif not ControllerConfig.__urlPatternRE.match(baseurl):
            errs.append('baseurl must match pattern %s.' % ControllerConfig.__urlPattern)
        if errs:
            raise Exception('\n'.join(errs))

    def __SetProperties(self, appname, baseurl):
        """
        * Get object properties from constructor parameters.
        """
        self.__appname = appname
        self.__baseurl = baseurl