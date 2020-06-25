#####################################
# LocalLargeDataJobPoster.py
#####################################
# Description:
# * Post a large number of jobs to DynamicETL.WebAPI, 
# and run DynamicETL.Service to insert all data into
# MetricsDyETL locally.

import clr
import copy
import os
import re
import requests
from shutil import copyfile
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
    __validModes = ['STG', 'UAT', 'QA', 'LOCAL']
    def __init__(self, webapipath, servicepath, serviceappsettings, config, etlpaths):
        """
        * Initialize object to begin posting for particular ETL with PostAllFiles().
        Inputs:
        * webapipath: Path to DynamicETL.WebAPI executable.
        * servicepath: Path to DynamicETL.Service executable.
        * serviceappsettings: JSON dictionary containing DynamicETL.Service appsettings-template.json
        or appsettings.json configurations with filled environment variables.
        * etlpaths: JSON dictionary mapping all etl names to drop locations.
        """
        LocalLargeDataJobPoster.__Validate(webapipath, servicepath, serviceappsettings, config, etlpaths)
        self.__webapipath = webapipath
        self.__servicepath = servicepath
        self.__config = copy.deepcopy(config)
        self.__serviceappsettings = copy.deepcopy(serviceappsettings)
        self.__modeserviceappsettings = None
        self.__etlpaths = copy.deepcopy(etlpaths)
        
    ##################
    # Interface Methods:
    ##################
    def PostAllFiles(self, etlname, datafolder, fileregex, testmode = "LOCAL", waitseconds = 200):
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
        else:
            if not etlname in self.__serviceappsettings['Etls']:
                errs.append('%s is not configured in the DynamicETL.Service appsettings.json file.' % etlname)
            # Ensure etl is configured in etlfilepaths.json:
            isAvailable = False
            for elem in self.__etlpaths['files']:
                if elem['subject'].lower() == etlname.lower():
                    isAvailable = True
                    break
            if not isAvailable:
                errs.append('%s ETL is not configured in etlfilepaths.json')
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
        if not isinstance(testmode, str):
            errs.append('testmode must be a string.')
        elif not testmode.upper() in LocalLargeDataJobPoster.__validModes:
            errs.append('testmode must be one of %s (case insensitive).' % ', '.join(LocalLargeDataJobPoster.__validModes))
        if not isinstance(waitseconds, (float, int)):
            errs.append('waitseconds must be numeric.')
        elif waitseconds <= 0:
            errs.append('waitseconds must be positive.')
        if errs:
            raise Exception('\n'.join(errs))
        testmode = testmode.upper()
        files = self.__GetAllMatchingFiles(datafolder, fileregex)
        if len(files) == 0:
            # Exit immediately if no matching files were found:
            return False
        self.__modeserviceappsettings = FillEnvironmentVariables(copy.deepcopy(self.__serviceappsettings), copy.deepcopy(self.__config), testmode)
        self.__modeetlpaths = FillEnvironmentVariables(copy.deepcopy(self.__etlpaths), copy.deepcopy(self.__config), testmode)
        if testmode == "LOCAL":
            self.__webapiurl = self.__modeserviceappsettings["EtlJobsUrl"]
            # Post all matching files to WebAPI and run Service locally:
            self.__OpenWebAPI()
            self.__PostAllJobs(etlname, files)
            self.__OpenService()
            self.__CloseAllInstances(waitseconds)
            return True
        else:
            # Drop all matching files to target location to be sucked up by ETL:
            self.__DropAllFiles(files, etlname)
            return True

    ##################
    # Private Helpers:
    ##################
    @staticmethod
    def __Validate(webapipath, servicepath, serviceappsettings, config, etlpaths):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(webapipath, str):
            errs.append('webapipath must be a string.')
        elif not webapipath.endswith('.dll'):
            errs.append('webapipath must point to a dll.')
        elif not os.path.exists(webapipath):
            errs.append('File at webapipath does not exist.')
        if not isinstance(servicepath, str):
            errs.append('servicepath must be a string.')
        elif not servicepath.endswith('.exe'):
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
        if not isinstance(etlpaths, dict):
            errs.append('etlpaths must be a JSON dictionary.')
        elif not 'files' in etlpaths:
            errs.append('etlpaths is missing the "files" attribute.')
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
        # Skip opening if already open:
        result = requests.get(self.__webapiurl)
        if result.status_code == 200:
            return
        else:
            pass
            #self.__webapiprocess = subprocess.Popen(self.__webapipath, stdout=subprocess.PIPE, creationflags=0x08000000)
            #self.__webapiprocess.wait()

    def __PostAllJobs(self, etlname, files):
        """
        * Post all files matching fileregex to
        DynamicETL.WebAPI.
        """
        for file in files:
            folderpath, filename = os.path.split(files[file])
            args = copy.deepcopy(LocalLargeDataJobPoster.__postargs)
            args['subject'] = etlname
            args['arg'] = args['arg'] % (folderpath, filename)
            args['fileName'] = filename
            result = requests.post(self.__modeserviceappsettings["EtlJobsUrl"], json = args)
            
    def __OpenService(self):
        """
        * Open DynamicETL.Service instance to 
        push all files to appropriate ETL table.
        """
        self.__serviceprocess = subprocess.Popen(self.__servicepath, stdout=subprocess.PIPE, creationflags=0x08000000)
        #self.__serviceprocess 

    def __CloseAllInstances(self, waitseconds):
        """
        * Close all external programs after waiting for some time.
        """
        sleep(waitseconds)
        #self.__webapiprocess.terminate()
        self.__serviceprocess.terminate()

    def __DropAllFiles(self, files, etlname):
        """
        * Drop all files to ETL drop location.
        """
        # Find the filepaths for corresponding ETL:
        outpath = None
        for elem in self.__modeetlpaths['files']:
            if elem['subject'].lower() == etlname.lower():
                outpath = os.path.split(elem['inbound'])[0]
                break
        if outpath is None:
            raise Exception("%s ETL is not present in etlfilepaths.json.")
        # Copy all files to location:
        for file in files:
            path, filename = os.path.split(files[file])
            copyfile(files[file], "%s\\%s" % (outpath, filename))