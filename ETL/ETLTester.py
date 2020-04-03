#####################################
# ETLTester.py
#####################################
# Description:
# * Test loaded data versus ETL test file.

from ETLJobLoader import ETLJobLoader
from pandas import DataFrame
import xlsxwriter

class ETLTester(object):
    """
    * Test new ETL created in .
    """
    def __init__(self, samplepath):
        self.__samplepath = samplepath

    def GenerateComparisonReport(self, sampleFilePath, tableName, outputPath, pKey = None):
        """
        * Generate report detailing differences between table rows
        and file rows.
        """
        # Notes: Sort first by string columns, then by int columns. 
        errs = []

        if errs:
            raise Exception('\n'.join(errs))

        reportWorkbook = Workbook()
        reportWorkbook.add_sheet("Table_Data")
        reportWorkbook.add_sheet("File_Data")
        for rowNum in range(0, fileData):
            pass

    @staticmethod
    def __AddRowsToSheet(data, sheet, dtFormat = None):
        rowNums = range(0, len(data))
        for rowNum in rowNums:
            for col in data.columns:
                if rowNum != 0:
                    sheet.write(rowNum, col, data[col][rowNum])
                else:
                    sheet.write(rowNum, col, data.columns[col])


