#####################################
# ETLJobLoader.py
#####################################
# Description:
# * Open DynamicETL.WebAPI, post ETL job and run DynamicETL.Service.
# Read logfile for issues if necessary.

import ctypes
from datetime import datetime
from enum import Enum
import json
import os
from pandas import DataFrame
import re
import requests
import threading
import time
import signal
import subprocess

class ETLJobLoader(object):
    """
    * Open DynamicETL.WebAPI, post ETL job and run DynamicETL.Service.
    Read logfile for issues if necessary.
    """
    __DefaultHost = 'https://localhost:23014/api/jobs'
    __ReqJSONFields = { 'id' : False, 'fileid' : False, 'subject' : False, 'arg' : False, 'filename' : False }
    __jsonRE = re.compile("{.+}")
    __serviceLogFile = re.compile('DynamicEtl.WebApi-.log')
    __webapiLogFile = re.compile('DynamicEtl.Service.log')
    ######################
    # Constructors/Destructor:
    ######################
    def __init__(self, webapiPath, servicePath, logpath, hostName = None):
        """
        * Initialize object that posts ETL jobs.
        Inputs:
        * webapiPath: File path to DynamicETL.WebAPI dll.
        * servicePath: File path to DynamicETL.Service exe.
        * logpath: Path to folder containing log files for both DynamicETL.WebAPI and Service.
        Optional Inputs:
        * hostName: URL for WebAPI host.
        """
        self.__Validate(webapiPath, servicePath, logpath, hostName)
        self.__host = hostName.strip() if not hostName is None else ETLJobLoader.__DefaultHost
        self.__webapipath = webapiPath.strip()
        self.__servicepath = servicePath.strip()
        self.__opened = False
        self.__runtime = None
        self.__etlname = None

    def __del__(self):
        """
        * Close DynamicETL.Webapi and DynamicETL.Service if opened.
        """
        self.__CloseApps()

    ######################
    # Interface Methods:
    ######################
    def RunETL(self, postargsjson):
        """
        * Open DynamicETL.WebAPI, post ETL job using passed json arguments 
        and run DynamicETL.Service.
        Inputs:
        * postargsjson: Dictionary containing WebAPI json post arguments for target ETL.
        """
        if not isinstance(postargsjson, dict):
            raise Exception('postargsjson must be a dictionary.')
        self.__etlname = None
        self.__webapiprocess = None
        try:
            # Validate json arguments:
            ETLJobLoader.__CheckJSONETL(postargsjson)
            self.__etlname = postargsjson['subject']
            # Open run main method in WebAPI, post json arguments
            # and run Service:
            self.__OpenWebAPI()
            self.__PostETL(postargsjson)
            self.__RunService()
            self.__runtime = datetime.now()
            self.__CloseApps()
        except Exception as ex:
            raise Exception('(ETLJobLoader) The following issues occurred: \n%s' % str(ex))

    def ReadLogFile(self, logpath):
        """
        * Read logfile associated with process, 
        summarize any issues that occurred with most recent
        etl job posting.
        Inputs:
        * logpath: Path to DynamicETL.Service/WebAPI/alt logfile.
        """
        errs = []
        if not isinstance(logpath, str):
            errs.append('logpath must be a string.')
        elif not os.path.isfile(logpath):
            errs.append('logpath does not point to valid file.')
        if self.__etlname is None or self.__runtime is None:
            errs.append('ETL has not been posted.')
        if errs:
            raise Exception('\n'.join(errs))

        issues = []
        # Read logfile, summarize issues:
        with open(logpath, 'r') as log:
            pass

        return '\n'.join(issues)

    @classmethod
    def RequiredJSONFields(cls):
        """
        * Return copy of required DynamicETL.WebAPI etl json fields.
        """
        return ETLJobLoader.__ReqJSONFields.copy()
        
    ######################
    # Private Helpers:
    ######################
    def __OpenWebAPI(self):
        """
        * Open DynamicETL.WebAPI instance if necessary.
        """
        # Determine if WebAPI is running:
        result = None
        try:
            result = requests.get(self.__host)
            if result.status_code == 200:
                return
        except Exception as ex:
            pass
        if not result is None and result.status_code != 200:
            raise Exception('Could not access WebAPI at %s. Status: %d.' % (self.__host, result.status_code))
        # Open WebAPI if not running:
        try:
            webapi = ctypes.cdll.LoadLibrary(self.__webapipath)
            self.__webapiprocess = webapi.Main()
        except Exception as ex:
            if '' in str(ex):
                raise Exception('')
            else:
                raise Exception('Failed to run WebApi. Reason: %s' % str(ex))
    def __PostETL(self, jsonobj):
        """
        * Open DynamicETL.WebAPI and post ETL using args in passed json file.
        """
        # Skip posting if already posted:
        result = requests.get(self.__host)
        if len(result.json()) != 0:
            for post in result.json():
                if 'identifier' in post and post['identifier'] == self.__etlname:
                    return
        # Post job to DynamicETL.WebAPI:
        result = requests.post(url = self.__host, data = jsonobj)
        if result.status_code != 200:
            raise Exception('Could not post to WebAPI at %s. Status: %d.' % (self.__host, result.status_code))

    def __RunService(self):
        """
        * Open DynamicETL.Service instance.
        """
        process = subprocess.Popen(self.__servicepath, shell = False, preexec_fn = os.setsid)
        time.sleep(10)
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)

    def __CloseApps(self):
        """
        * Close DynamicETL.WebAPI and Service instances.
        """
        if not self.__webapiprocess is None:
            os.killpg(os.getpgid(self.__webapiprocess.pid), signal.SIGTERM)

    def __CheckETLPosted(self):
        """
        * Check that ETL was successfully posted.
        """
        result = requests.get(self.__host)
    
    def __Validate(self, webapiPath, servicePath, logpath, hostName):
        """
        * Validate construction parameters. Raise exceptions if issues
        occurred.
        """
        errs = []
        if not isinstance(webapiPath, str):
            errs.append('webapiPath must be a string.')
        elif not os.path.exists(webapiPath):
            errs.append('webapiPath does not exist.')
        elif not webapiPath.endswith('.dll'):
            errs.append('webapiPath must point to a dll.')
        if not isinstance(servicePath, str):
            errs.append('servicePath must be a string.')
        elif not os.path.exists(servicePath):
            errs.append('servicePathdoes not exist.')
        elif not servicePath.endswith('.exe'):
            errs.append('servicePath must point to an exe.')
        if not isinstance(logpath, str):
            errs.append('logpath must be a string.')
        elif not os.path.isdir(logpath):
            errs.append('logpath must point to a folder.')
        elif not os.path.exists(logpath):
            errs.append('logpath does not exist.')
        if not hostName is None and not isinstance(hostName, str):
            errs.append('hostName must be a string or None.')
        if errs:
            raise Exception('\n'.join(errs))

    @classmethod
    def __CheckJSONETL(cls, jsonObj):
        """
        * Ensure all required attributes are in the json object and valid.
        """
        errs = []
        reqFields = ETLJobLoader.__ReqJSONFields.copy()
        normJSON = { col.lower() : jsonObj[col] for col in jsonObj }
        for field in normJSON:
            if field in reqFields:
                reqFields[field] = True

        missing = [reqFields[field] for field in reqFields if not reqFields[field]]
        if missing:
            errs.append(''.join(['Missing the following JSON fields:{', ','.join(missing), '}']))
        if reqFields['arg'] and 'FilePath' not in normJSON['arg']: 
            errs.append('arg must include filepath.')
        elif reqFields['arg'] and not os.path.exists(normJSON['arg'].split('FilePath')[1].strip("':'").strip("'}")):
            errs.append('file at arg::FilePath does not exist.')
        if errs:
            raise Exception('\n'.join(errs))


