#####################################
# ControllerConfig.py
#####################################
# Description:
# * Object to store flask API configurations
# from .json file for generation at runtime.

import re

class ControllerConfig:
    """
    * Object to store flask API configurations
    from .json file for generation at runtime.
    """
    __req = set(['appname', 'baseurl'])
    __urlPattern = 'http://.+'
    __urlPatternRE = re.compile(__urlPattern)
    def __init__(self, section):
        """
        * Convert dictionary key and value into
        an APIConfig object.
        Inputs:
        * name: Name of API.
        * config: Dictionary object containing all
        required configurations for API.
        """
        ControllerConfig.__Validate(name, config)
        self.__SetProperties(name, config)

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
        return ControllerConfig.__req

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
            missing = ControllerConfig._req - set(section)
            if missing:
                errs.append('section is missing the following required attributes: %s.' % ','.join(missing))
            if 'appname' in section and not isinstance(section['appname'], str):
                errs.append('section::appname must be a string.')
            if 'baseurl' in section:
                if not isinstance(section['baseurl'], str):
                    errs.append('section::baseurl must be a string.')
                elif not ControllerConfig.__urlPattern.match(section):
                    errs.append('section::baseurl must match pattern %s.' % ControllerConfig.__urlPattern)
        if errs:
            raise Exception('\n'.join(errs))

    def __SetProperties(self, section):
        """
        * Get object properties from constructor parameters.
        """
        self.__appname = section['appname']
        self.__baseurl = section['baseurl']