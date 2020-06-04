#####################################
# FileTransferConfig.py
#####################################
# Description:
# * Gathers attributes from xml files containing 
# filetransfer configurations for a particular etl.

from bs4 import BeautifulSoup as Soup
from datetime import datetime
import dateutil.parser as dtparser
from Filepaths.FileTransferSource import FileTransferSource
from Filepaths.FileTransferDestination import FileTransferDestination
import os
from Utilities.Helpers import StringIsDT

class FileTransferConfig:
    """
    * Gathers attributes from xml files containing 
    filetransfer configurations for a particular etl.
    """
    __startTag = '<Transfer xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"></Transfer>'
    def __init__(self, filepath = None):
        """
        * Initialize empty configuration or 
        convert passed xml file at filepath.
        Optional:
        * filepath: Path to xml file containing 
        filetransfer configuration.
        """
        FileTransferConfig.__Validate(filepath)
        if not filepath is None:
            self.__ConvertFile(filepath)
        else:
            self.__DefaultInitialize()

    ##################
    # Interface Methods:
    ##################
    def GenerateXML(self, filepath):
        """
        * Generate XML file at specified path using attributes.
        Inputs:
        * filepath: String path to xml file wish to output to.
        """
        if not isinstance(filepath, str):
            raise Exception('filepath must be a string.')
        elif not filepath.endswith('.xml'):
            raise Exception('filepath must point to xml file.')
        soup = Soup(FileTransferConfig.__startTag)
        # Add all non-Source attributes
        attrs = [attr for attr in dir(self) if not attr.startswith('_') and not attr in ["Sources", "RetryFailedOnly"]]
        for attr in attrs:
            tag = soup.new_tag(attr)
            tag.insert(0, NavigableString(getattr(self, attr)))
            soup.Transfer.append(tag)

        # Add Sources:

        # Output to file:


    def AsJSON(self):
        """
        * Return attributes as json object.
        """
        out = {}
        out['TransferId'] = self.TransferID
        out['TransferTimeLimitSeconds'] = self.TransferTimeLimitSeconds
        out['LastAttempt'] = self.LastAttempt
        out['PollingFrequencySeconds'] = self.PollingFreqSeconds
        out['IsDeleted'] = self.IsDeleted
        out['IsDisabled'] = self.IsDisabled
        out['Description'] = self.Description
        out['LastUpdateTime'] = self.LastUpdateTime
        out['ExpirationTime'] = self.ExpirationTime
        out['LastModifiedByUser'] = self.LastModifiedByUser
        out['Sources'] = self.Sources
        out['RetriedFailedOnly'] = self.RetriedFailedOnly
        return out

    def ConvertJSON(self, jsonobj):
        """
        * Convert json object into FileTransferConfig object.
        """
        self.__DefaultInitialize()
        self.TransferID = out['TransferId']
        self.TransferTimeLimitSeconds = out['TransferTimeLimitSeconds']
        self.LastAttempt = out['LastAttempt']
        self.PollingFreqSeconds = out['PollingFrequencySeconds']
        self.IsDeleted = out['IsDeleted']
        self.IsDisabled = out['IsDisabled']
        self.Description = out['Description']
        self.LastUpdateTime = out['LastUpdateTime']
        self.ExpirationTime = out['ExpirationTime']
        self.LastModifiedByUser = out['LastModifiedByUser']
        self.Sources = out['Sources']
        self.RetriedFailedOnly = out['RetriedFailedOnly']
        
    ##################
    # Private Helpers:
    ##################
    def __ConvertFile(self, filepath):
        """
        * Convert passed xml file to transfer object.
        """
        filevals = FileTransferConfig.__CleanFile(filepath)
        soup = Soup(filevals, features = "html.parser")
        self.TransferID = soup.find("transferid").text
        self.TransferTimeLimitSeconds = soup.find("transfertimelimitseconds").text
        self.LastAttempt = soup.find("lastattempt").text
        self.PollingFreqSeconds = soup.find("pollingfrequencyseconds").text
        self.IsDeleted = soup.find("isdeleted").text
        self.IsDisabled = soup.find("isdisabled").text
        self.Description = soup.find("description").text
        self.LastUpdateTime = soup.find("lastupdatetime").text
        self.ExpirationTime = soup.find("expirationtime").text
        self.LastModifiedByUser = soup.find("lastmodifiedbyuser").text
        self.__ParseSources(soup)
        self.RetriedFailedOnly = soup.find("retriedfailedonly").text

    def __ParseSources(self, soup):
        """
        * Parse the "Sources" attribute.
        """
        sources = soup.find_all("source")
        for source in sources:
            converted = FileTransferSource(source)
            desttags = source.find_all("destination")
            destinations = []
            for tag in desttags:
                destinations.append(FileTransferDestination(tag))
            self.__sources[converted.Path] = desttags

    @staticmethod
    def __Validate(filepath):
        """
        * Validate constructor arguments.
        """
        errs = []
        if not filepath is None and not isinstance(filepath, str):
            errs.append('filepath must be a string.')
        elif not filepath.endswith('.xml'):
            errs.append('filepath must point to an xml file.')
        elif not os.path.exists(filepath):
            errs.append('filepath does not exist.')
        if errs:
            raise Exception('\n'.join(errs))

    @staticmethod
    def __CleanFile(filepath):
        """
        * Clean strings in file before converting into
        BeautifulSoup object.
        """
        cleanedLines = []
        with open(filepath, 'r') as f:
            lines = f.readlines()
            for line in lines:
                # Remove non-ascii characters:
                cleanedLine = ''.join([ch.strip('\x00\n') for ch in line if ord(ch) < 128])
                if cleanedLine:
                    cleanedLines.append(cleanedLine)
        # Fix <source/> and <destination/> tags being ill defined:
        soup = Soup('\n'.join(cleanedLines), features = 'html.parser')
        sources = soup.find_all('source')
        sourcetags = set([attr.lower() for attr in dir(FileTransferSource) if not attr.startswith('_')])
        desttags = set([attr.lower() for attr in dir(FileTransferDestination) if not attr.startswith('_')])
        for source in sources:
            currsourcetags = []
        return soup

    def __DefaultInitialize(self):
        """
        * Initialize all members to default values.
        """
        self.__transferid = 0
        self.__transfertimelimitseconds = None
        self.__lastattempt = None
        self.__pollingfrequencyseconds = None
        self.__isdeleted = None
        self.__isdisabled = None
        self.__description = None
        self.__lastupdate = None
        self.__expirationtime = None
        self.__lastmodifieduser = None
        self.__sources = None
        self.__retriedfailedonly = None

    ##################
    # Properties:
    ##################
    @property
    def TransferID(self):
        return self.__transferid
    @property
    def TransferTimeLimitSeconds(self):
        return self.__transfertimelimitseconds
    @property
    def LastAttempt(self):
        return self.__lastattempt
    @property
    def PollingFreqSeconds(self):
        return self.__pollingfrequencyseconds
    @property
    def IsDeleted(self):
        return self.__isdeleted
    @property
    def IsDisabled(self):
        return self.__isdisabled
    @property
    def Description(self):
        return self.__description
    @property
    def LastUpdateTime(self):
        return self.__lastupdate
    @property
    def ExpirationTime(self):
        return self.__expirationtime
    @property
    def LastModifiedByUser(self):
        return self.__lastmodifieduser
    @property
    def Sources(self):
        return self.__sources.copy()
    @property
    def RetriedFailedOnly(self):
        return self.__retriedfailedonly
    @TransferID.setter
    def TransferID(self, val):
        if not isinstance(val, (int, float, str)) and not val is None:
            raise Exception('TransferID must be an integer string, numeric or None.')
        elif isinstance(val, (int, float)) and not val > 0:
            raise Exception('TransferID must be positive.')
        self.__transferid = int(val)
    @TransferTimeLimitSeconds.setter
    def TransferTimeLimitSeconds(self, val):
        if not isinstance(val, (int, float, str)) and not val is None:
            raise Exception('TransferTimeLimitSeconds must be an integer string, numeric or None.')
        elif isinstance(val, (int, float)) and not val > 0:
            raise Exception('TransferTimeLimitSeconds must be positive.')
        self.__transfertimelimitseconds = int(val)
    @LastAttempt.setter
    def LastAttempt(self, val):
        if not isinstance(val, (str, datetime)) and not val is None:
            raise Exception('LastAttempt must be a convertible string, datetime object or None.')
        elif isinstance(val, str) and not StringIsDT(val):
            raise Exception('LastAttempt could not be converted to datetime object.')
        elif isinstance(val, str):
            val = None if not val else dtparser.parse(val)
        self.__lastattempt = val
    @PollingFreqSeconds.setter
    def PollingFreqSeconds(self, val):
        if not isinstance(val, (int, float, str)) and not val is None:
            raise Exception('PollingFreqSeconds must be an integer string, numeric or None or None.')
        elif isinstance(val, (int, float)) and not val > 0:
            raise Exception('PollingFreqSeconds must be positive.')
        elif isinstance(val, str):
            val = None if not val else int(val)
        self.__pollingfrequencyseconds = int(val)
    @IsDeleted.setter
    def IsDeleted(self, val):
        if not isinstance(val, (str, bool)) and not val is None:
            raise Exception('IsDeleted must be boolean, "true"/"false" string or None.')
        elif isinstance(val, str):
            val = val.lower().strip()
            if not val in ['true', 'false']:
                raise Exception('IsDeleted must be boolean, "true"/"false" string or None.')
            val = True if val == 'true' else False
        self.__isdeleted = val
    @IsDisabled.setter
    def IsDisabled(self, val):
        if not isinstance(val, (str, bool)) and not val is None:
            raise Exception('IsDisabled must be boolean.')
        elif isinstance(val, str):
            val = val.lower().strip()
            if not val in ['true', 'false']:
                raise Exception('IsDisabled must be boolean, "true"/"false" string or None.')
            val = True if val == 'true' else False
        self.__isdisabled = val
    @Description.setter
    def Description(self, val):
        if not isinstance(val, str) and not val is None:
            raise Exception('Description must be a string or None.')
        self.__description = val
    @LastUpdateTime.setter
    def LastUpdateTime(self, val):
        if not isinstance(val, (str, datetime)) and not val is None:
            raise Exception('LastUpdateTime must be a convertible string, datetime object or None.')
        elif isinstance(val, str):
            if not val:
                val = None
            elif not StringIsDT(val):
                raise Exception('LastUpdateTime could not be converted to datetime object.')
            else:
                val = dtparser.parse(val)
        self.__lastupdate = val
    @ExpirationTime.setter
    def ExpirationTime(self, val):
        if not isinstance(val, (str, datetime)) and not val is None:
            raise Exception('ExpirationTime must be a convertible string, datetime object or None.')
        elif isinstance(val, str):
            if not val:
                val = None
            elif not StringIsDT(val):
                raise Exception('LastUpdateTime could not be converted to datetime object.')
            else:
                val = dtparser.parse(val)
        self.__expirationtime = val
    @LastModifiedByUser.setter
    def LastModifiedByUser(self, val):
        if not isinstance(val, str):
            raise Exception('LastModifiedByUser must be a string.')
        self.__lastmodifieduser = val
    @Sources.setter
    def Sources(self, val):
        if not isinstance(val, dict) and not val is None:
            raise Exception('Sources must be a dictionary mapping {Sources -> [Destinations]} or None.')
        elif any([not isinstance(key, FileTransferSource) for key in val]):
            raise Exception('All of the keys in Sources must be FileTransferSource objects.')
        elif any([not isinstance(value, List) for value in val.values()]):
            raise Exception('All of values in Sources must be lists of FileTransferDestination objects.')
        self.__sources = val
    @RetriedFailedOnly.setter
    def RetriedFailedOnly(self, val):
        if not isinstance(val, (bool,str)) and not val is None:
            raise Exception('RetriedFailedOnly must be a boolean, string or None.')
        elif isinstance(val, str):
            val = val.lower()
            if not val in ['true', 'false']:
                raise Exception('RetriedFailedOnly must be "true"/"false" if string.')
            else:
                self.__retriedfailedonly = True if val == 'true' else False
        elif isinstance(val, bool) or val is None:
            self.__retriedfailedonly = val

