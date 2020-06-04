#####################################
# FileTransferConfig.py
#####################################
# Description:
# * Gathers attributes from xml files containing 
# filetransfer configurations for a particular etl.

from bs4 import BeautifulSoup as Soup
import dateutil.parser as dtparser
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
        self.__transferid = int(soup.find("transferid").text)
        self.__transfertimelimitseconds = int(soup.find("transfertimelimitseconds").text)
        self.__lastattempt = dtparser.parse(soup.find("lastattempt").text)
        self.__pollingfrequencyseconds = int(soup.find("pollingfrequencyseconds").text)
        self.__isdeleted = True if soup.find("isdeleted").text.lower().strip() == 'true' else False
        self.__isdisabled = True if soup.find("isdisabled").text.lower().strip() == 'true' else False
        self.__description = soup.find("description").text
        self.__lastupdatetime = dtparser.parse(soup.find("lastupdatetime").text)
        self.__expirationtime = dtparser.parse(soup.find("expirationtime").text)
        self.__lastmodifieduser = soup.find("lastmodifieduser").text
        self.__ParseSources(soup)
        self.__retriedfailedonly = True if soup.find("retriedfailedonly").text.lower().strip() == 'true' else False

    def __ParseSources(soup):
        """
        * Parse the "Sources" attribute.
        """
        sources = soup.find_all("source")
        for source in sources:
            self.__sources[''] = {}

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
        return '\n'.join(cleanedLines)

    def __DefaultInitialize(self):
        """
        * Initialize all members to default values.
        """
        self.__transferid = 0
        self.__transfertimelimitseconds = 0
        self.__lastattempt = None
        self.__pollingfrequencyseconds = 0
        self.__isdeleted = False
        self.__isdisabled = False
        self.__description = ''
        self.__lastupdate = None
        self.__expirationtime = None
        self.__lastmodifieduser = ''
        self.__sources = {}
        self.__retriedfailedonly = False

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
        if not isinstance(val, (int, float)):
            raise Exception('TransferID must be numeric.')
        elif not val > 0:
            raise Exception('TransferID must be positive.')
        self.__transferid = int(val)
    @TransferTimeLimitSeconds.setter
    def TransferTimeLimitSeconds(self, val):
        if not isinstance(val, (int, float)):
            raise Exception('TransferTimeLimitSeconds must be numeric.')
        elif not val > 0:
            raise Exception('TransferTimeLimitSeconds must be positive.')
        self.__transfertimelimitseconds = int(val)
    @LastAttempt.setter
    def LastAttempt(self, val):
        if not isinstance(val, (str, datetime)):
            raise Exception('LastAttempt must be a convertible string or datetime object.')
        elif isinstance(val, str) and not StringIsDT(val):
            raise Exception('LastAttempt could not be converted to datetime object.')
        elif isinstance(val, str):
            val = dtparser.parse(val)
        self.__lastattempt = val
    @PollingFreqSeconds.setter
    def PollingFreqSeconds(self, val):
        if not isinstance(val, (int, float)):
            raise Exception('PollingFreqSeconds must be numeric.')
        elif not val > 0:
            raise Exception('PollingFreqSeconds must be positive.')
        self.__pollingfrequencyseconds = int(val)
    @IsDeleted.setter
    def IsDeleted(self, val):
        if not isinstance(val, bool):
            raise Exception('IsDeleted must be boolean.')
        self.__isdeleted = val
    @IsDisabled.setter
    def IsDisabled(self, val):
        if not isinstance(val, bool):
            raise Exception('IsDisabled must be boolean.')
        self.__isdisabled = val
    @Description.setter
    def Description(self, val):
        if not isinstance(val, str):
            raise Exception('Description must be a string.')
        self.__description = val
    @LastUpdateTime.setter
    def LastUpdateTime(self):
        if not isinstance(val, (str, datetime)):
            raise Exception('LastUpdateTime must be a convertible string or datetime object.')
        elif isinstance(val, str) and not StringIsDT(val):
            raise Exception('LastUpdateTime could not be converted to datetime object.')
        elif isinstance(val, str):
            val = dtparser.parse(val)
        self.__lastupdate = val
    @ExpirationTime.setter
    def ExpirationTime(self, val):
        self.__expirationtime = val
    @LastModifiedByUser.setter
    def LastModifiedByUser(self, val):
        if not isinstance(val, str):
            raise Exception('LastModifiedByUser must be a string.')
        self.__lastmodifieduser = val
    @Sources.setter
    def Sources(self, val):
        FileTransferConfig.__VerifySource(val)
        self.__sources = val
    @RetriedFailedOnly.setter
    def RetriedFailedOnly(self, val):
        if not isinstance(val, bool):
            raise Exception('RetriedFailedOnly must be a boolean.')
        self.__retriedfailedonly = val
    


#############################
# TransferSources:
#############################
class TransferSources:
    """
    * Attributes in the "Sources" tag of a 
    transfer XML config.
    """
    def __init__(self, tag):
        """
        * Create new TransferSources object using passed
        xml tag or json object.
        """
        TransferSources.__Validate(tag)
        if isinstance(tag, str):
            pass
        elif isinstance(tag, dict):
            pass

    ################
    # Properties:
    ################
    


