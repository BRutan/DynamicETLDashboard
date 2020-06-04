#####################################
# FileTransferSource.py
#####################################
# Description:
# * Object contains all attributes in "source" tag
# in file transfer xml file.


class FileTransferSource:
    """
    * Contains all attributes in "source" tag
    in file transfer xml file.
    """
    def __init__(self, tag = None):
        FileTransferSource.__Validate(tag)
        if isinstance(tag, str):
            tag = None
        if not tag is None:
            self.__ConvertSourceTag(tag)
        else:
            self.__DefaultInitialize()
    ################
    # Properties:
    ################
    @property
    def SourceID(self):
        return self.__sourceid
    @property
    def TransferID(self):
        return self.__transferid
    @property
    def TransferTypename(self):
        return self.__transfertypename
    @property
    def FileInfoID(self):
        return self.__fileinfoid
    @property
    def Path(self):
        return self.__path
    @property
    def TypeNameMask(self):
        return self.__typenamemask
    @property
    def ShouldDelete(self):
        return self.__shoulddelete
    @property
    def LimitGetHours(self, val):
        return self.__limitgethours
    @SourceID.setter
    def SourceID(self, val):
        if isinstance(val, str):
            if not val.isnumeric():
                raise Exception('SourceID must be an integer if string.')
            else:
                self.__sourceid = int(val)
        elif not val is None and not isinstance(val, int):
            raise Exception('SourceID must be an integer, integer string or None.')
        elif isinstance(val, int) or val is None:
            self.__sourceid = val
    @TransferID.setter
    def TransferID(self, val):
        if isinstance(val, str):
            if not val.isnumeric():
                raise Exception('TransferID must be an integer if string.')
            else:
                self.__transferid = int(val)
        elif not val is None and not isinstance(val, int):
            raise Exception('TransferID must be an integer, integer string or None.')
        elif isinstance(val, int) or val is None:
            self.__transferid = val
    @TransferTypename.setter
    def TransferTypename(self, val):
        if isinstance(val, str) or val is None:
            self.__transferid = val
        else:
            raise Exception('TransferTypeName must be a string or None.')
    @FileInfoID.setter
    def FileInfoID(self, val):
        if isinstance(val, str):
            if not val.isnumeric():
                raise Exception('FileInfoID must be an integer if string.')
            else:
                self.__fileinfoid = int(val)
        elif not val is None and not isinstance(val, int):
            raise Exception('FileInfoID must be an integer, integer string or None.')
        elif isinstance(val, int) or val is None:
            self.__fileinfoid = val
    @Path.setter
    def Path(self, val):
        if isinstance(val, str) or val is None:
            self.__path = val
        else:
            raise Exception('Path must be a string or None.')
    @TypeNameMask.setter
    def TypeNameMask(self, val):
        if isinstance(val, str) or val is None:
            self.__typenamemask = val
        else:
            raise Exception('TypeNameMask must be a string or None.')
    @ShouldDelete.setter
    def ShouldDelete(self, val):
        if isinstance(val, str):
            val = val.lower()
            if not val in ['true', 'false']:
                raise Exception('ShouldDelete must be "true"/"false" if string.')
            else:
                self.__shoulddelete = True if val == 'true' else False
        elif not val is None and not isinstance(val, bool):
            raise Exception('ShouldDelete must be a boolean or "true"/"false" (case insensitive).')
        elif isinstance(val, bool) or val is None:
            self.__shoulddelete = val
    @LimitGetHours.setter
    def LimitGetHours(self, val):
        if isinstance(val, str):
            if not val.isnumeric():
                raise Exception('LimitGetHours must be an integer string.')
            else:
                self.__limitgethours = int(val)
        elif not val is None and not isinstance(val, bool):
            raise Exception('LimitGetHours must be an integer, an integer string or None.')
        elif isinstance(val, int) or val is None:
            self.__limitgethours = val

    ################
    # Private Helpers:
    ################
    def __ConvertSourceTag(self, tag):
        """
        * Convert BeautifulSoup tag to object.
        """
        self.SourceID = tag.find("sourceid").text
        self.TransferID = tag.find("transferid").text
        self.TransferTypename = tag.find("transfertypename").text
        self.FileInfoID = tag.find("fileinfoid").text
        self.Path = tag.find("path").text
        self.TypeNameMask = tag.find("typenamemask").text
        self.ShouldDelete = tag.find("shoulddelete").text
        self.LimitGetHours = tag.find("limitgethours").text

    def __DefaultInitialize(self, tag):
        """
        * Initialize empty object.
        """
        self.SourceID = None
        self.TransferID = None
        self.TransferTypename = None
        self.FileInfoID = None
        self.Path = None
        self.TypeNameMask = None
        self.ShouldDelete = None
        self.LimitGetHours = None

    @staticmethod
    def __Validate(tag):
        """
        * Validate constructor arguments.
        """
        if not tag is None and not isinstance(tag, (str)):
            raise Exception('tag must be an html string or BeautifulSoup tag if provided.')
        