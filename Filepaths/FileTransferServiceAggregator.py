#####################################
# FileTransferServiceAggregator.py
#####################################
# Description:
# * Aggregate all filepaths from FileTransferService to allow
# fast lookup for data source and output paths.

from bs4 import Beautifulsoup as Soup
import json
import os
from selenium import webdriver

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
        self.__driver = None
        self.__paths = None
        self.__ftsurl = ftsurl
        self.__transfersjson = {}
        self.__PullFromFTS(chromedriverpath)

    def __del__(self):
        """
        * Close chromedriver instance and delete downloaded xml files
        if necessary.
        """
        self.__Cleanup()

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
    def __PullFromFTS(self, chromedriverpath):
        """
        * Open Chrome instance with Selenium to pull all existing
        filetransferservice paths.
        """
        try:
            self.__ConnectChromeDriver()
            self.__MaximizeTransfersPerSheet()
            self.__DownloadTargetTransfers()
            self.__AggregateTransfers()
        except Exception as ex:
            pass

    def __ConnectChromeDriver(self):
        """
        * Generate new Chrome instance, 
        """
        chromeOptions = webdriver.ChromeOptions()
        chromeOptions.add_experimental_option('useAutomationExtension', False)
        self.__driver = webdriver.Chrome('Misc\\chromedriver.exe', chrome_options = chromeOptions)
        self.__driver.get(ftsurl)
        
    def __MaximizeTransfersPerSheet(self):
        """
        * Maximize number of transfers per sheet to minimize page 
        switching.
        """
        selbox = self.__driver.find_element_by_class_name('ui-pg-selbox')
        options = selbox.find_elements_by_tag_name('option')
        targetopt = None
        for option in options:
            if targetopt is None or (not targetopt is None and option.text.isnumeric() and int(option.text) > int(targetopt.text)):
                targetopt = option
        targetopt.click()

    def __DownloadTargetTransfersToTemp(driver):
        pageswitch = self.__driver.find_element_by_xpath('//*[@id="xferpager_center"]/table/tbody/tr/td[4]')
        currpage = pageswitch.find_element_by_id('sp_1_xferpager').text
        maxpage = 24
        while currpage != maxpage:
            elems = [elem for elem in driver.find_elements_by_tag_name('tr') if elem.get_attribute('role') == 'row']   
            for elem in elems:
                pass

                pageswitch.find_element_by_id('sp_1_xferpager').text
    def __AggregateTransfers(self, paths):
        pass

    def __Cleanup(self):
        """
        * Delete downloaded xml files and close
        Chrome window if necessary.
        """
        if not self.__paths is None:
            for path in self.__paths:
                os.rmdir(path)
            self.__paths = None
        if not self.__driver is None:
            self.__driver.close()

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
