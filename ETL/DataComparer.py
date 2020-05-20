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
        compData, missingColsMsg = DataComparer.__Compare(data_test, data_valid, ignoreCols, pKey)
        reportWB = xlsxwriter.Workbook(reportPath)
        DataComparer.__GenerateSummaryPage(compData,reportWB,missingColsMsg)
        DataComparer.__GenerateDiffPage(compData,reportWB)
        reportWB.close()

    ####################
    # Private Helpers:
    ####################
    @classmethod
    def __GenerateSummaryPage(cls, compData, wb, missingColsMsg):
        """
        * Generate summary page in workbook.
        """
        wb.add_sheet('Summary')
        wb.write(0, 0, 'Missing Columns')
        wb.write(0, 1, missingColsMsg)
        wb.write(1, 0, '# of Differing Rows')
        wb.write(1, 1, len(compData))
        # Write # of differences for each column:
        wb.write(2, 0, '# of Differences by Column')
        for num, col in enumerate(compData.columns):
            wb.write(3, num, col)
            wb.write(3, num, len([val for val in compData[col] if val]))

    @classmethod
    def __GenerateDiffPage(cls, compData, wb):
        """
        * Generate sheet detailing specific differences in 
        column values.
        """
        wb.add_sheet('Differences')
        # Write all differing columns:
        compData.to_excel(wb, 'Differences')
        #for rowNum in range(0, len(compData)):
        #    for colNum, col in enumerate(compData.columns):
        #        if rowNum != 0:
        #            wb.write(rowNum, colNum, compData[col][rowNum - 1])
        #        else:
        #            wb.write(rowNum, colNum, col)
    @classmethod
    def __Compare(cls, data_test, data_valid, ignoreCols, pKey):
        """
        * Return dataframe containing rows where datasets differ.
        """
        missingColsMsg = None
        data_test = data_test.rename(columns={col : col.lower() for col in data_test.columns})
        data_valid = data_valid.rename(columns={col : col.lower() for col in data_valid.columns})
        if not ignoreCols is None:
            ignoreCols = set([col.lower() for col in ignoreCols])
            data_test = data_test[[col for col in data_test.columns if not col.lower() in ignoreCols]]
            data_valid = data_test[[col for col in data_valid.columns if not col.lower() in ignoreCols]]
        # Remove columns in data_valid not in data_test:
        missingCols = set(data_valid.columns) - set(data_test.columns)
        if missingCols:
            missingColsMsg = ','.join(missingCols)
            data_valid = data_valid[[col for col in data_valid if not col in missingCols]]
        # Perform comparison:
        if not pKey is None:
            # Compare using primary key:
            pKey = pKey.lower()
            diff = { col : [] for col in data_test.columns }
            validPKeys = set(data_valid[pKey])
            for row in range(0, len(data_test)):
                target = data_test.iloc[row][pKey]
                if target not in validPKeys:
                    # Write entire row if pkey value not present:
                    for col in data_test.columns:
                        diff[col].append(data_test.iloc[row][col])
                else:
                    # Write all column values that do not match:
                    match = data_valid.loc[data_valid[pKey] == target].values[0]
                    rowDiff = { col : None for col in data_test.columns }
                    appendDiff = False
                    for num, col in enumerate(data_test.columns):
                        if col != pKey and data_test[col][row] != match[num]:
                            rowDiff[col] = '%s vs %s' % (data_test[col][row], match[num])
                            appendDiff = True
                    # Append differing values if differences occurred:
                    if appendDiff:
                        rowDiff[pKey] = target
                        for col in rowDiff:
                            diff[col].append(rowDiff[col])
            return DataFrame(diff), missingColsMsg
        else:
            # Return all rows in data_test not present in data_valid:
            test_tuples = set(data_test.itertuples())
            valid_tuples = set(data_valid.itertuples())
            diff = valid_tuples - test_tuples
            return DataFrame(diff, columns = data_test.columns), missingColsMsg

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
        elif not ignoreCols is None and any([not isinstance(val, str) for val in ignoreCols]):
            errs.append('ignoreCols must be list of strings if provided.')
        if not pKey is None and not isinstance(pKey, str):
            errs.append('pKey must be a string if provided.')
        if isinstance(pKey, str) and isinstance(data_test, DataFrame) and not pKey.lower() in [col.lower() for col in data_test.columns]:
            errs.append('pKey not present in data_test.')
        if isinstance(pKey, str) and isinstance(data_valid, DataFrame) and not pKey.lower() in [col.lower() for col in data_valid.columns]:
            errs.append('pKey not present in data_valid.')
        if errs:
            raise Exception('\n'.join(errs))