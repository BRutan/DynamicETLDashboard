#####################################
# FileTransferDestination.py
#####################################
# Description:
# * Contains attributes in "destination" tag in filetransfer xml.

from bs4 import BeautifulSoup as Soup

class FileTransferDestination:
    """
    * Contains attributes in "destination" tag in filetransfer xml.
    """
    def __init__(self, tag = None):
        """
        * Get attributes from tag.
        Optional Inputs:
        * tag: String containing html/xml or beautifulsoup
        tag.
        """
        FileTransferDestination.__Validate(tag)
        if isinstance(tag, str):
            tag = Soup(tag, features = "html.parser")
        if not tag is None:
            self.__ConvertDestinationTag(tag)
        else:
            self.__DefaultInitialize()
    ###################
    # Properties:
    ###################
    @property
    def DestinationID(self):
        return self.__destinationid
    @property
    def SourceID(self):
        return self.__sourceid
    @property
    def TransferTypeName(self):
        return self.__transfertypename
    @property
    def FileInfoID(self):
        return self.__fileinfoid
    @property
    def Path(self):
        return self.__path
    @property
    def FileNameMask(self):
        return self.__filenamemask
    @DestinationID.setter
    def DestinationID(self, val):
        if not isinstance(val, (str, int)) and not val is None:
            raise Exception('DestinationID must be a integer string, integer or None.')
        elif isinstance(val, str):
            if not val.isnumeric():
                raise Exception('DestinationID must be a integer string, integer or None.')
            else:
                self.__destinationid = int(val)
        elif isinstance(val, int) or val is None:
            self.__destinationid = val
    @SourceID.setter
    def SourceID(self):
        if not isinstance(val, (str, int)) and not val is None:
            raise Exception('SourceID must be a integer string, integer or None.')
        elif isinstance(val, str):
            if not val.isnumeric():
                raise Exception('SourceID must be a integer string, integer or None.')
            else:
                self.__sourceid = int(val)
        elif isinstance(val, int) or val is None:
            self.__sourceid = val
    @TransferTypeName.setter
    def TransferTypeName(self, val):
        if not isinstance(val, str) and not val is None:
            raise Exception('TransferTypeName must be a string or None.')
        self.__transfertypename = val
    @FileInfoID.setter
    def FileInfoID(self, val):
        if not isinstance(val, (str, int)) and not val is None:
            raise Exception('FileInfoID must be a integer string, integer or None.')
        elif isinstance(val, str):
            if not val.isnumeric():
                raise Exception('FileInfoID must be a integer string, integer or None.')
            else:
                self.__fileinfoid = int(val)
        elif isinstance(val, int) or val is None:
            self.__fileinfoid = val
    @Path.setter
    def Path(self, val):
        if not isinstance(val, str) and not val is None:
            raise Exception('Path must be a string or None.')
        self.__path = val
    @FileNameMask.setter
    def FileNameMask(self, val):
        if not isinstance(val, str) and not val is None:
            raise Exception('FileNameMask must be a string or None.')
        self.__filenamemask = val
    ###################
    # Private Helpers:
    ###################
    def __ConvertDestinationTag(self, tag):
        """
        * Convert html tag to object.
        """
        self.DestinationID = tag.find('destinationid').text
        self.SourceID = tag.find('sourceid').text
        self.TransferTypeName = tag.find('transfertypename').text
        self.FileInfoID = tag.find('fileinfoid').text
        self.Path = tag.find('path')
        self.FileNameMask = tag.find('filenamemask').text
        
    def __DefaultInitialize(self):
        """
        * Initialize empty object.
        """
        self.DestinationID = None
        self.SourceID = None
        self.TransferTypeName = None
        self.FileInfoID = None
        self.Path = None
        self.FileNameMask = None

    @staticmethod
    def __Validate(tag):
        """
        * Validate constructor parameters.
        """
        if not tag is None and not (isinstance(tag, str) or str(type(tag)) == "<class 'bs4.element.Tag'>"):
            raise Exception('tag must be None, a string containing html or a BeautifulSoup tag.')
