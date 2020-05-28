#####################################
# FileTransferServiceAggregator.py
#####################################
# Description:
# * Aggregate all filepaths from
# FileTransferService to allow
# fast lookup for data source and output
# paths.

from bs4 import Beautifulsoup as Soup
import json
import selenium

class FileTransferServiceAggregator:
    """
    * Aggregate all filepaths from FileTransferService to allow
    fast lookup for data source and output paths.
    """
    def __init__(self, ftsurl):
        """
        * Open Selenium instance and aggregate
        all filetransfers.
        """
        FileTransferServiceAggregator.__Validate(ftsurl)
        self.__transfersjson = {}
        self.__PullFromFTS(ftsurl)

    ####################
    # Interface Methods:
    ####################
    def OutputLookup(self):
        """
        * Output lookup file into filetransferconfig.json.
        """
        if not os.path.exists('AppsettingsFiles\\'):
            raise Exception('Local AppsettingsFiles\\ folder does not exist.')
        json.dump(self.__transfersjson, open('AppsettingsFiles\\filetransferconfig.json', 'wb'), sort_keys = True)

    ####################
    # Private Helpers:
    #################### 
    def __PullFromFTS(self, ftsurl):
        """
        * Open Chrome instance with Selenium to pull all existing
        filetransferservice paths.
        """
        try:
            runSelenium = None
        except Exception as ex:
            errs.append('Missing chromedriver from PATH.')
            errs.append('Please download from https://sites.google.com/a/chromium.org/chromedriver/downloads')
            errs.append('and add to PATH variable.')
            raise Exception('\n'.join(errs))
