#####################################
# DataComparer.py
#####################################
# Description:
# * Generate report comparing two datasets.

import os
from pandas import DataFrame
import xlsxwriter

class DataComparer(object):
    """
    * Compare two DataFrames.
    """
    def __init__(self):
        """
        * Create empty object that can generate reports.
        """
        pass
    ####################
    # Interface Methods:
    ####################
    @classmethod
    def GenerateComparisonReport(cls, reportPath, data_test, data_valid, ignoreCols = None, pKey = None):
        """
        * Generate report detailing differences between table rows
        and file rows.
        Inputs:
        * reportPath: Path to output report. Must point to xlsx file.
        * data_test: DataFrame containing test data.
        * data_valid: DataFrame containing valid data to compare against.
        Optional:
        * ignoreCols: List of columns (strings) to ignore when comparing. 
        * pKey: If provided, rows will be compared where primary key column is the same.
        """
        # Validate parameters:
        DataComparer.__Validate(reportPath, data_test, data_valid, ignoreCols, pKey)
        # Generate report:
        compData = DataComparer.__Compare(data_test, data_valid, ignoreCols, pKey)
        reportWB = xlsxwriter.Workbook(reportPath)
        DataComparer.__GenerateSummaryPage(compData,reportWB)
        DataComparer.__GenerateDiffPage(compData,reportWB)
        reportWB.close()

    ####################
    # Private Helpers:
    ####################
    @classmethod
    def __GenerateSummaryPage(cls, compData, wb):
        """
        * Generate summary page in workbook.
        """
        wb.add_sheet('Summary')

    @classmethod
    def __GenerateDiffPage(cls, compData, wb):
        """
        * Generate sheet detailing specific differences in 
        column values.
        """
        wb.add_sheet('Differences')

    @classmethod
    def __AddRowsToSheet(cls, data, sheet, dtFormat = None):
        rowNums = range(0, len(data))
        for rowNum in rowNums:
            for col in data.columns:
                if rowNum != 0:
                    sheet.write(rowNum, col, data[col][rowNum])
                else:
                    sheet.write(rowNum, col, data.columns[col])
    @classmethod
    def __Compare(cls, data_test, data_valid, ignoreCols, pKey):
        """
        * Return dataframe containing rows where datasets differ.
        """
        if not ignoreCols is None:
            ignoreCols = set(ignoreCols)
            data_test = data_test[[col for col in data_test.columns if not col in ignoreCols]]
            data_valid = data_test[[col for col in data_valid.columns if not col in ignoreCols]]
        if not pKey is None:
            pass

    @classmethod
    def __Validate(cls, reportPath, data_test, data_valid, ignoreCols, pKey):
        """
        * Validate parameters for main function.
        """
        errs = []
        if not isinstance(reportPath, str):
            errs.append('reportPath must be a string.')
        elif not reportPath.endswith('.xlsx'):
            errs.append('reportPath must point to xlsx file.')
        if not isinstance(data_test, DataFrame):
            errs.append('data_test must be a DataFrame.')
        if not isinstance(data_valid, DataFrame):
            errs.append('data_valid must be a DataFrame.')
        if not ignoreCols is None and not isinstance(ignoreCols, list):
            errs.append('ignoreCols must be a list of strings if provided.')
        elif not any([not isinstance(val, str) for val in ignoreCols]):
            errs.append('ignoreCols must be list of strings if provided.')
        if not pKey is None and not isinstance(pKey, str):
            errs.append('pKey must be a string if provided.')
        if errs:
            raise Exception('\n'.join(errs))
