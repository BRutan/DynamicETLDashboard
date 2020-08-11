#####################################
# GenerateFTSPaths.py
#####################################
# Description:
# * Automatically generate FileTransferService XML 
# schema for each version of ETL (QA, UAT, STG) 
# using provided attributes.

from bs4 import BeautifulSoup as Soup
import json
import re

class FTSSchemaGenerator(object):
    """
    * Generate XML file ready to upload into gsfts.
    """
    __pathTypes = { 'QA' : False, 'STG' : False, 'UAT' : False }
    __outPath = '{0}{1}.xml'
    __attrJsonFile = 'fts.json'
    __defaultSchema = 'default.xml'
    __defaultOutputPath = '\\\\wanlink.us\\dfsroot\\APPS\\GCM Feeds\\{Type}\\Risk Dashboard\\{DataPath}\\'
    __defaultAttrJSON = { 'Name' : {'Source' : '\\\\wanlink.us\\dfsroot\\APPS\\GCM Feeds\\{0}', 
                          'Production' : '\\\\wanlink.us\\dfsroot\\APPS\\GCM Feeds\\{0}',
                          'DataPath' : '',
                          'FilenameMask' : '', 
                          'DateMask' : 'dateMask:yyyyMMdd' } }
    __sourceRE = re.compile('source_\d+', re.IGNORECASE)
    def __init__(self):
        """
        * Generate new fts schema for upload into //gsfts//.
        """
        xmlData = Soup(FTSSchemaGenerator.__defaultSchema, 'lxml')
        attrData = json.load(FTSSchemaGenerator.__attrJsonFile)

    ################
    # Interface Functions:
    ################
    @staticmethod
    def GenerateAttributeJson():
        """
        *
        """
        path = 'ftsattrs.json'
        json.dump(FTSSchemaGenerator.__defaultAttrJSON, open(path, 'w'))

    ################
    # Private Helpers:
    ################
    def __CreatePathsFile(self):
        """
        * Create file detailing new paths to be created.
        """
        pass

    def __GenerateXML(self, attrJson, xmlData):
        """
        * Generate XML to upload into gsfts.
        """
        # Get all sources:
        sources = []
        for key in attrJson:
            if FTSPathGenerator.__sourceRE.match(key):
                sources.append(key)


if __name__ == '__main__':
    print ('Generating default FTS json.')
    FTSSchemaGenerator.GenerateAttributeJson()