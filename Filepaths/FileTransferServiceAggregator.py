#####################################
# FileTransferServiceAggregator.py
#####################################
# Description:
# * Aggregate all filepaths from FileTransferService to allow
# fast lookup for data source and output paths.

from bs4 import Beautifulsoup as Soup
import json
import os
import re
from selenium import webdriver
from Utilities.Helpers import IsRegex

class FileTransferServiceAggregator:
    """
    * Aggregate all filepaths from FileTransferService to allow
    fast lookup for data source and output paths.
    """
    __reType = type(re.compile('a'))
    def __init__(self, ftsurl, chromedriverpath, groupregex):
        """
        * Open Selenium instance and aggregate
        all filetransfers.
        Inputs:
        * ftsurl: URL to gsfts web portal.
        * chromedriverpath: Path to chromedriver.exe.
        * groupregex: Regular expression object or string to determine which transfers to pull 
        (ex: RiskDashboard). 
        """
        FileTransferServiceAggregator.__Validate(ftsurl, chromedriverpath, groupregex)
        self.__driver = None
        self.__paths = None
        self.__ftsurl = ftsurl
        self.__driverpath = chromedriverpath
        self.__targetregex = groupregex if isinstance(groupregex, FileTransferServiceAggregator.__reType) else re.compile(groupregex)
        self.__transfersjson = {}
        self.__PullFromFTS()

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
    def __PullFromFTS(self):
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

    def __DownloadTargetTransfers(self):
        """
        * Pull all targeted XML transfer configs from 
        gsfts url.
        """
        self.__paths = []
        movebutton = self.__driver.find_element_by_xpath('//*[@id="next_xferpager"]/span')
        pageindicator = self.__driver.find_element_by_xpath('//*[@id="xferpager_center"]/table/tbody/tr/td[4]')
        currpage = int(pageindicator)
        maxpage = int(pageindicator.find_element_by_id('sp_1_xferpager').text)
        while currpage <= maxpage:
            elems = [elem for elem in self.__driver.find_elements_by_tag_name('tr') if elem.get_attribute('role') == 'row']   
            for elem in elems:
                cells = elem.get_elements_by_tag_name('td')
                if self.__targetregex.match(cells[2].text):
                    elem.get_element_by_class_name("ui-icon ui-icon-arrowreturnthick-1-s").click()
                self.__paths.append('')
            # Proceed to next page:
            movebutton.click()
            currpage += 1

    def __AggregateTransfers(self):
        """
        * Convert XML configs into json object.
        """
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
    def __Validate(ftsurl, chromedriverpath, groupregex):
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
        if not isinstance(groupregex, (str, reType)):
            errs.append('groupregex must be a string or regular expression object.')
        elif isinstance(groupregex, str) and not IsRegex(groupregex):
            errs.append('groupregex must be a valid regular expression string.')
        if errs:
            raise Exception('\n'.join(errs))