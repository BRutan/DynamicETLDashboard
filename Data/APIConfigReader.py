#####################################
# APIConfig.py
#####################################
# Description:
# * Read API configs for usage in
# derived APIReceivers.

import json
import os
import re

class APIConfigReader:
    """
    * Read API configs for usage in
    derived APIReceivers.
    """
    def __init__(self, apiJson):
        """
        * Convert api json into APIs.
        """
        pass



    ################
    # Private Helpers:
    ################
    def __Read(self, path):
        """
        * Read in all configurec API attributes
        from json file.
        """
        pass

        
    @staticmethod
    def __Validate(path):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(path, str):
            errs.append('path must be a string.')
        elif not path.endswith('.json'):
            errs.append('path must point to a .json file.')
        elif not os.path.exists(path):
            errs.append('file at path does not exist.')
        if errs:
            raise Exception('\n'.join(errs))


class APIConfig:
    """
    * Section detailing configured 
    APIs, stored in APIConfigReader.
    """
    __attrRE = re.compile('')
    def __init__(self, attr):
        """
        * Generate APIConfig from passed
        attribute.
        Inputs:
        * attr: string containing attributes.
        """
        APIConfigReader.__Validate(attr)
        self.__SetProperties(attr)
        
    ################
    # Properties:
    ################
    @property
    def APIName(self):
        return self.__apiname
    @property
    def URL(self):
        return self.__url
    ################
    # Interface Methods:
    ################
    @classmethod
    def IsValid(cls, attr):
        """
        * Ensure attribute is properly formatted.
        Inputs:
        * attr:
        Returns (true/false, message) to indicate that attribute is properly formatted
        and specific issues if improperly formatted.
        """
        pass

    ################
    # Private Helpers:
    ################
    def __SetProperties(self, attr):
        """
        * Set properties using attribute.
        """
        pass
    @staticmethod
    def __Validate(attr):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(attr, str):
            errs.append('attr must be a string.')
        if errs:
            raise Exception('\n'.join(errs))
