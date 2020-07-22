#####################################
# FileTransferServiceAggregator.py
#####################################
# Description:
# * Aggregate all filepaths from FileTransferService to allow
# fast lookup for data source and output paths.

from bs4 import BeautifulSoup as Soup
from ETL.FileTransferConfig import FileTransferConfig
import json
import os
import re
from selenium import webdriver
from time import sleep
from Utilities.FileConverter import FileConverter
from Utilities.Helpers import IsRegex, LoadJsonFile

class FileTransferServiceAggregator:
    """
    * Aggregate all filepaths from FileTransferService to allow
    fast lookup for data source and output paths.
    """
    __xferFileSig = re.compile('TransferId_\d+.xml')
    __reType = type(re.compile('a'))
    def __init__(self, ftsurl, etlpathsjson, chromedriverpath, groupregex):
        """
        * Open Selenium instance and aggregate
        all filetransfers.
        Inputs:
        * ftsurl: URL to gsfts web portal.
        * etlpathsjson: Json object filled with data in etlfilepaths.json.
        * chromedriverpath: Path to chromedriver.exe.
        * groupregex: Regular expression object or string to determine which transfers to pull 
        (ex: RiskDashboard). 
        """
        FileTransferServiceAggregator.__Validate(ftsurl, etlpathsjson, chromedriverpath, groupregex)
        self.__driver = None
        self.__paths = None
        self.__ftsurl = ftsurl
        self.__etlpathsjson = etlpathsjson
        # Get paths to all versions of chromedriver.exe:
        self.__GetChromeDriverPaths(chromedriverpath)
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
    @staticmethod
    def GenerateETLFilePathsJson(filewatcherappsettingspath):
        """
        * Convert FileWatcher appsettings-template.json file into
        an etlfilepaths.json file (replaces a few environment
        variables).
        Inputs:
        * filewatcherappsettingspath: path to FileWatcher appsettings-template.json
        file.
        """
        if not isinstance(filewatcherappsettingspath, str):
            raise Exception('filewatcherappsettingspath must be a string.')
        elif not filewatcherappsettingspath.endswith('json'):
            raise Exception('filewatcherappsettingspath must point to json file.')
        elif not os.path.exists(filewatcherappsettingspath):
            raise Exception('filewatcherappsettingspath does not exist.')
        try:
            fwargs = LoadJsonFile(filewatcherappsettingspath)
        except Exception as ex:
            errs = ['Could not load filewatcherappsettingspath json file.']
            errs.append('Reason: %s' % str(ex))
            raise Exception('\n'.join(errs))
        if not 'files' in fwargs:
            raise Exception('Missing required "files" key in json file.')
        out = {}
        out['files'] = []
        for etlgroup in fwargs['files']:
            etlgroup['inbound'] = etlgroup['inbound'].replace('{Inbound_Folder}', '{CC_Source_Path}')
            out['files'].apend(etlgroup)
        json.dump(out, open(os.getcwd() + '\\AppsettingsFiles\\etlfilepaths.json', 'wb'))
        
    def OutputLookup(self, output, filename = 'filetransferconfig.json'):
        """
        * Output lookup file into filetransferconfig.json.
        """
        errs = []
        if not isinstance(outputfolder, str):
            errs.append('outputfolder must be a string.')
        elif not os.path.isdir(outputfolder):
            errs.append('outputfolder does not point to valid directory.')
        if not isinstance(filename, str):
            errs.append('filename must be a string.')
        elif not filename.endswith('.json'):
            errs.append('filename must point to .json file')
        if errs:
            raise Exception('\n'.join(errs))
        json.dump(self.__transfersjson, open('%s\\%s' % (output, filename), 'wb'), sort_keys = True)

    ####################
    # Private Helpers:
    ####################
    def __GetChromeDriverPaths(self, chromedriverpath):
        """
        * Get all chromedriver.exe versions stored
        locally.
        """
        folder, file = os.path.split(chromedriverpath)
        folder, folderRE = os.path.split(folder)
        folderRE = re.compile(folderRE)
        folders = FileConverter.GetAllFolderPaths(folder, folderRE)
        self.__chromedriverpaths = [os.path.join(folder,file) for folder in folders]

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
            self.__Cleanup()
            raise ex

    def __ConnectChromeDriver(self):
        """
        * Generate new Chrome instance, 
        """
        # Output downloaded transfers to temporary directory:
        drive = re.search('[A-Z]:', os.getcwd())[0]
        self.__downloaddir = "%s\\Users\\%s\\Downloads" % (drive, os.getlogin().lower())
        if not os.path.exists(self.__downloaddir):
            raise Exception('Download directory %s does not exist.' % self.__downloaddir)
        chromeOptions = webdriver.ChromeOptions()
        chromeOptions.add_experimental_option('useAutomationExtension', False)
        #chromeOptions.add_argument("download.default_directory=%s" % self.__tempdir)
        # Use the first valid chromedriver.exe given installed version of Google Chrome:
        fail = True
        for path in self.__chromedriverpaths:
            try:
                self.__driver = webdriver.Chrome(path, chrome_options = chromeOptions)
                self.__driver.get(self.__ftsurl)
                fail = False
                break
            except:
                pass
        if fail:
            raise Exception('Could not use any chromedriver.exe to open selenium.')
        sleep(3)
        
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
        sleep(3)

    def __DownloadTargetTransfers(self):
        """
        * Pull all targeted XML transfer configs from 
        gsfts url.
        """
        movebutton = self.__driver.find_element_by_xpath('//*[@id="next_xferpager"]/span')
        pageindicator = self.__driver.find_element_by_xpath('//*[@id="xferpager_center"]/table/tbody/tr/td[4]')
        currpage = 1
        maxpage = int(pageindicator.find_element_by_id('sp_1_xferpager').text)
        while currpage <= maxpage:
            elems = [elem for elem in self.__driver.find_elements_by_tag_name('tr') if elem.get_attribute('role') == 'row']   
            for elem in elems:
                cells = elem.find_elements_by_tag_name('td')
                if self.__targetregex.match(cells[1].text):
                    # Click button to download file to temporary location:
                    a = cells[9].find_element_by_tag_name('a')
                    a.find_element_by_tag_name("span").click()
            # Proceed to next page:
            movebutton.click()
            movebutton = self.__driver.find_element_by_xpath('//*[@id="next_xferpager"]/span')
            currpage += 1
        # Wait so all files are downloaded:
        sleep(3)
        self.__driver.close()
        self.__driver = None
        # Get all paths to downloaded xml files:
        self.__paths = FileConverter.GetAllFilePaths(self.__downloaddir, FileTransferServiceAggregator.__xferFileSig)

    def __AggregateTransfers(self):
        """
        * Convert XML configs into json objects, link up to ETLs.
        """
        for path in self.__paths:
            transferconfig = FileTransferConfig(self.__paths[path]) 
            # Lookup ETL associated to sources:
            for etl in self.__etlpathsjson:
                if etl == 1:
                    self.__transfersjson[etlname] = transferconfig.Sources
                    break
            
    def __Cleanup(self):
        """
        * Delete downloaded xml files and close
        Chrome window if necessary.
        """
        if not self.__paths is None:
            for filename in self.__paths:
                os.remove(self.__paths[filename])
        if not self.__driver is None:
            self.__driver.close()
            self.__driver = None

    @staticmethod
    def __Validate(ftsurl, etlpathsjson, chromedriverpath, groupregex):
        """
        * Validate constructor parameters.
        """
        errs = []
        if not isinstance(ftsurl, str):
            errs.append('ftsurl must be a string.')
        if not isinstance(etlpathsjson, dict):
            errs.append('etlpathsjson must be a json dictionary.')
        if not isinstance(chromedriverpath, str):
            errs.append('chromedriverpath must be a string.')
        elif not chromedriverpath.endswith('.exe'):
            errs.append('chromedriverpath must point to an executable.')
        if not isinstance(groupregex, (str, FileTransferServiceAggregator.__reType)):
            errs.append('groupregex must be a string or regular expression object.')
        elif isinstance(groupregex, str) and not IsRegex(groupregex):
            errs.append('groupregex must be a valid regular expression string.')
        if errs:
            raise Exception('\n'.join(errs))
