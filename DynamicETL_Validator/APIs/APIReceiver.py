#####################################
# APIReceiver.py
#####################################
# Description:
# * Abstract base class for derived
# APIReceivers that perform tasks
# when receive data.

from abc import ABC, abstractmethod, abstractproperty
from flask import Flask, Blueprint
import re

class APIReceiver(ABC):
    """
    * Abstract base class for derived
    APIReceivers that perform tasks
    when receive data.
    """
    __invalidNames = set([re.compile('flask')])
    __urlPattern = r'http://.+'
    __urlRE = re.compile(__urlPattern)
    def __init__(self, name, url):
        """
        * Generate instance of Flask class,
        creating api at provided url.
        Inputs:
        * name: Name of API. Must be a string.
        * url: URL for API. Must be a string and 
        match URL pattern.
        """
        APIReceiver.__Validate(name, url)
        self.__SetProperties(name, url)
        
    ################
    # Properties:
    ################
    @property
    def Name(self):
        return self.__name
    @property
    def URL(self):
        return self.__url

    ################
    # Private Helpers:
    ################
    @classmethod
    def IsValidUrl(cls, url):
        """
        * Test to see if passed url is valid.
        Inputs:
        * url: string url.
        """
        if not isinstance(url, str):
            raise Exception('url must be a string.')
        return APIReceiver.__urlPattern.match(url)

    def __SetProperties(self, name, url):
        """
        * Set object properties from constructor 
        parameters.
        """
        self.__name = name
        self.__url = url
        
    @staticmethod
    def __Validate(name, url):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(name, str):
            errs.append('name must be a string.')
        if not isinstance(url, str):
            errs.append('url must be a string.')
        elif not APIReceiver.__urlRE.match(url):
            errs.append('url must have following pattern: %s' % APIReceiver.__urlPattern)
        if errs:
            raise Exception('\n'.join(errs))