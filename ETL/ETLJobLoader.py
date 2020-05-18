#####################################
# ETLJobLoader.py
#####################################
# Description:
# * Post ETL job using POST request in local .json file.

import connexion
import ctypes
from enum import Enum
import json
import os
from pandas import DataFrame
import re
import requests

class ETLJobLoader(object):
    """
    * Load ETL Job to server.
    """
    __DefaultHost = 'https://localhost:23014/api/jobs'
    __ReqJSONFields = { 'id' : False, 'fileid' : False, 'subject' : False, 'arg' : False, 'filename' : False }
    __jsonRE = re.compile("{.+}")
    ######################
    # Constructors/Destructor:
    ######################
    def __init__(self, webapiPath, hostName = None):
        """
        * Initialize object that posts ETL jobs.
        Inputs:
        * webapiPath: Path to WebAPI dll.
        Optional Inputs:
        * hostName: URL for WebAPI host.
        """
        errs = []
        if not isinstance(webapiPath, str):
            errs.append('webapiPath must be a string.')
        elif not os.path.exists(webapiPath):
            errs.append('webapiPath does not exist.')
        elif not webapiPath.endswith('.dll'):
            errs.append('webapiPath must point to a dll.')
        if hostName and not isinstance(hostName, str):
            errs.append('hostName must be a string or None.')
        if errs:
            raise Exception('\n'.join(errs))

        self.__host = hostName.strip() if not hostName is None else ETLJobLoader.__DefaultHost
        self.__webapipath = webapiPath.strip()
        self.__opened = False

    def __del__(self):
        """
        * Close Webapi if opened.
        """
        self.CloseWebAPI()
    ######################
    # Interface Methods:
    ######################
    def PostETL(self, jsonpath):
        """
        * Run WebAPI, post ETL in json file at path to target server.
        Inputs:
        * jsonpath: Path to json file containing post arguments.
        """
        if not isinstance(jsonpath, str):
            raise Exception('jsonpath must be a string.')
        elif not jsonpath.endswith('json'):
            raise Exception('jsonpath must point to json file.')
        elif not os.path.exists(jsonpath):
            raise Exception('jsonpath does not exists.')
        
        # Validate json arguments:
        jsonArgs = json.load(open(jsonpath, 'rb'))
        ETLJobLoader.__CheckJSONETL(jsonArgs)
        # Open WebAPI and 
        # Post job to server:
        try:
            result = requests.post(url = self.__host, data = jsonArgs)
        except Exception as ex:
            print(str(ex))
        # Load new JSON object and post:
        

    def CheckETLPosted(self):
        """
        * Check that ETL has been posted already.
        """
        result = requests.get(self.__host)

    def OpenWebAPI(self):
        """
        * Close WebAPI instance.
        """
        if not self.__opened:
            pass

    def CloseWebAPI(self):
        """
        * Close WebAPI instance.
        """
        if self.__opened:
            pass
        
    ######################
    # Static Helpers:
    ######################
    @classmethod
    def RequiredJSONFields(cls):
        """
        * 
        """
        return ETLJobLoader.__ReqJSONFields.copy()
        
    @classmethod
    def __CheckJSONETL(cls, jsonObj):
        """
        * Ensure all required attributes are in the json object and valid.
        """
        reqFields = ETLJobLoader.__ReqJSONFields.copy()
        normJSON = { col.lower() : col[jsonObj] for col in jsonObj }
        for field in normJSON:
            if field in reqFields:
                reqFields[field] = True

        missing = [reqFields[field] for field in reqFields if not reqFields[field]]
        if missing:
            errs.append(''.join(['Missing the following JSON fields:{', ','.join(missing), '}']))
        if reqFields['arg'] and 'filepath' not in normJSON['arg']: 
            errs.append('arg must include filepath.')
        elif reqFields['arg'] and not os.path.exists(normJSON['arg']['filepath']):
            errs.append('file at filepath in json file does not exist.')
                
        if errs:
            raise Exception('\n'.join(errs))

class SQLCredentials(object):
    """
    * Credentials used to log into sql server.
    """
    __paramToCred = { "" : "" } 
    def __init__(self, **kwargs):
        pass



