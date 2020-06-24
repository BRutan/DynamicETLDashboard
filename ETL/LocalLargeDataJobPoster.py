#####################################
# LocalLargeDataJobPoster.py
#####################################
# Description:
# * Post a large number of jobs to DynamicETL.WebAPI, 
# and run DynamicETL.Service to insert all data into
# MetricsDyETL locally.

import copy
import os
import re
import requests
from time import sleep
import subprocess
from Utilities.FileConverter import FileConverter
from Utilities.Helpers import FillEnvironmentVariables, IsRegex

class LocalLargeDataJobPoster:
    """
    * Post a large number of jobs to DynamicETL.WebAPI, 
    and run DynamicETL.Service to insert all data into
    MetricsDyETL locally. 
    """
    __postargs = {"id": 10, "fileid": 10, "subject": None, "arg": "{'FilePath':'%s\\%s'}", "fileName": None}
    __reType = type(re.compile(''))
    def __init__(self, webapipath, servicepath, serviceappsettings, config = None):
        """
        * Initialize object to begin posting for particular ETL with PostAllFiles().
        Inputs:
        * webapipath: Path to DynamicETL.WebAPI executable.
        * servicepath: Path to DynamicETL.Service executable.
        * serviceappsettings: JSON dictionary containing DynamicETL.Service appsettings-template.json
        or appsettings.json configurations with filled environment variables.
        Optional:
        * config: JSON dictionary to fill serviceappsettings environment variables with.
        * waittime: Number of seconds to wait to allow data to be pulled into DynamicETL.Service.
        """
        self.__Validate(webapipath, servicepath, serviceappsettings, config)
        self.__webapipath = webapipath
        self.__servicepath = servicepath
        self.__serviceappsettings = serviceappsettings
        if not config is None:
            self.__serviceappsettings = FillEnvironmentVariables(self.__serviceappsettings, config, "LOCAL")

    ##################
    # Interface Methods:
    ##################
    def PostAllFiles(self, etlname, datafolder, fileregex, waitseconds = 200):
        """
        * Open DynamicETL.WebAPI, post all files located in datapath folder matching fileregex, 
        and run DynamicETL.Service to load data into local tables.
        Inputs:
        * etlname: String etl name. Must be configured in serviceappsettings.
        * datafolder: String folder containing all data files for ETL.
        * fileregex: Regular expression string or regex object to match files in datafolder.
        Optional:
        * waitseconds: Number of seconds to wait before closing DynamicETL.Service and WebAPI.
        """
        errs = []
        if not isinstance(etlname, str):
            errs.append('etlname must be a string.')
        elif not etlname in self.__serviceappsettings['Etls']:
            errs.append('%s is not configured in the DynamicETL.Service appsettings.json file.' % etlname)
        if not isinstance(datafolder, str):
            errs.append('datafolder must be a string.')
        elif not os.path.isdir(datafolder):
            errs.append('datafolder does not point to a valid folder.')
        if isinstance(fileregex, str):
            if not IsRegex(fileregex):
                errs.append('fileregex is not a valid regular expression string.')
            else:
                fileregex = re.compile(fileregex)
        elif not isinstance(fileregex, LocalLargeDataJobPoster.__reType):
            errs.append('fileregex must be a regular expression string or regular expression object.')
        if not isinstance(waitseconds, (float, int)):
            errs.append('waitseconds must be numeric.')
        elif waitseconds <= 0:
            errs.append('waitseconds must be positive.')
        if errs:
            raise Exception('\n'.join(errs))

        # Post all matching files:
        files = self.__GetAllMatchingFiles(datafolder, fileregex)
        if len(files) == 0:
            # Exit immediately if no matching files were found:
            return False
        self.__OpenWebAPI()
        self.__PostAllJobs(etlname, files)
        self.__OpenService()
        self.__CloseAllInstances(waitseconds)
        return True

    ##################
    # Private Helpers:
    ##################
    def __Validate(webapipath, servicepath, serviceappsettings, config):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(webapipath, str):
            errs.append('webapipath must be a string.')
        elif webapipath.endswith('.exe'):
            errs.append('webapipath must point to an executable.')
        elif not os.path.exists(webapipath):
            errs.append('File at webapipath does not exist.')
        if not isinstance(servicepath, str):
            errs.append('servicepath must be a string.')
        elif servicepath.endswith('.exe'):
            errs.append('servicepath must point to an executable.')
        elif not os.path.exists(servicepath):
            errs.append('File at servicepath does not exist.')
        if not isinstance(serviceappsettings, dict):
            errs.append('serviceappsettings must be a JSON dictionary.')
        else:
            if not 'EtlJobsUrl' in serviceappsettings:
                errs.append('serviceappsettings is missing the "EtlJobsUrl" key.')
            if not 'Etls' in serviceappsettings:
                errs.append('serviceappsettings is missing the "Etls" key.')
        if not config is None and not isinstance(config, dict):
            errs.append('config must be a JSON dictionary.')
        if errs:
            raise Exception('\n'.join(errs))

    def __GetAllMatchingFiles(self, datafolder, fileregex):
        """
        * Return all files matching regular expression for use
        in ETL.
        """
        return FileConverter.GetAllFilePaths(datafolder, fileregex)
    
    def __OpenWebAPI(self):
        """
        * Open DynamicETL.WebAPI instance.
        """
        self.__webapiprocess = subprocess.Popen(self.__webapipath, stdout=subprocess.PIPE, creationflags=0x08000000)
        self.__webapiprocess.wait()

    def __PostAllJobs(self, etlname, files):
        """
        * Post all files matching fileregex to
        DynamicETL.WebAPI.
        """
        for path in files:
            folderpath, filename = os.path.split(path)
            args = copy.deepcopy(LocalLargeDataJobPoster.__postargs)
            args['subject'] = etlname
            args['arg'] = args['arg'] % (folderpath, filename)
            args['fileName'] = filename
            result = requests.post(self.__serviceappsettings["EtlJobsUrl"], json = args)
            
    def __OpenService(self):
        """
        * Open DynamicETL.Service instance to 
        push all files to appropriate ETL table.
        """
        self.__serviceprocess = subprocess.Popen(self.__servicepath, stdout=subprocess.PIPE, creationflags=0x08000000)
        self.__serviceprocess.wait()

    def __CloseAllInstances(self, waitseconds):
        """
        * Close all external programs after waiting for some time.
        """
        sleep(waitseconds)
        self.__webapiprocess
        self.__serviceprocess
