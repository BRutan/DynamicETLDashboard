#####################################
# FileTransferServiceAggregator.py
#####################################
# Description:
# * Aggregate all filepaths from FileTransferService to allow
# fast lookup for data source and output paths.

from bs4 import Beautifulsoup as Soup
import json
import os
import selenium

class FileTransferServiceAggregator:
    """
    * Aggregate all filepaths from FileTransferService to allow
    fast lookup for data source and output paths.
    """
    def __init__(self, ftsurl, chromedriverpath):
        """
        * Open Selenium instance and aggregate
        all filetransfers.
        """
        FileTransferServiceAggregator.__Validate(ftsurl, chromedriverpath)
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
    def __PullFromFTS(self, ftsurl, chromedriverpath):
        """
        * Open Chrome instance with Selenium to pull all existing
        filetransferservice paths.
        """
        try:
            selenobj = selenium.webdriver.Chrome(chromedriverpath)
        except Exception as ex:
            errs.append('Missing chromedriver from PATH.')
            errs.append('Please download from https://sites.google.com/a/chromium.org/chromedriver/downloads')
            errs.append('and add to PATH variable.')
            raise Exception('\n'.join(errs))
        # Export all xml configs to temporary location:
        tempLoc = "Temp"
        if not os.path.exists(tempLoc):
            os.mkdir(tempLoc)

    @staticmethod
    def __Validate(ftsurl, chromedriverpath):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(ftsurl, str):
            errs.append('ftsurl must be a string.')
        if not isinstance(chromedriverpath, str):
            errs.append('chromedriverpath must be a string.')
        elif not chromedriverpath.endswith('.exe'):
            errs.append('chromedriverpath must point to an executable.')
        elif not os.path.exists(chromedriverpath):
            errs.append('chromedriverpath file does not exist.')
        if errs:
            raise Exception('\n'.join(errs))
