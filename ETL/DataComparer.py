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
        * ignoreCols: Iterable of columns (strings) to ignore when comparing. 
        * pKey: String or iterable to determine which rows to compare.
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
        summarySheet = wb.add_worksheet('Summary')
        summarySheet.write(0, 0, 'Missing Columns')
        summarySheet.write(0, 1, missingColsMsg)
        summarySheet.write(1, 0, '# of Differing Rows')
        summarySheet.write(1, 1, len(compData))
        # Write # of differences for each column:
        summarySheet.write(2, 0, '# of Differences by Column')
        for num, col in enumerate(compData.columns):
            summarySheet.write(3, num, col)
            summarySheet.write(4, num, len([val for val in compData[col] if val]))

    @classmethod
    def __GenerateDiffPage(cls, compData, wb):
        """
        * Generate sheet detailing specific differences in 
        column values.
        """
        # Do not generate sheet if no differences occurred.
        if len(compData) == 0:
            return
        diffSheet = wb.add_worksheet('Differences')
        # Write all differing columns:
        for rowNum in range(0, len(compData)):
            for colNum, col in enumerate(compData.columns):
                if rowNum != 0:
                    diffSheet.write(rowNum, colNum, compData[col][rowNum - 1])
                else:
                    # Write headers:
                    diffSheet.write(rowNum, colNum, col)
    @classmethod
    def __Compare(cls, data_test, data_valid, ignoreCols, pKey):
        """
        * Return dataframe containing rows where datasets differ.
        """
        missingColsMsg = None
        data_test = data_test.rename(columns={col : col.lower() for col in data_test.columns}).fillna('')
        data_valid = data_valid.rename(columns={col : col.lower() for col in data_valid.columns}).fillna('')
        if not ignoreCols is None:
            ignoreCols = set([col.lower() for col in ignoreCols])
            data_test = data_test[[col for col in data_test.columns if not col.lower() in ignoreCols]]
            data_valid = data_valid[[col for col in data_valid.columns if not col.lower() in ignoreCols]]
        # Remove columns in data_valid not in data_test:
        missingCols = set(data_valid.columns) - set(data_test.columns)
        if missingCols:
            missingColsMsg = ','.join(missingCols)
            data_valid = data_valid[[col for col in data_valid if not col in missingCols]]
        # Perform comparison:
        if not pKey is None:
            # Compare using primary key(s):
            pKey = [pKey.lower()] if isinstance(pKey, str) else [key.lower() for key in pKey]
            data_test = data_test.set_index(list(pKey))
            data_valid = data_valid.set_index(list(pKey))
            diff = { col : [] for col in data_test.columns }
            diff.update({col : [] for col in data_test.index.names})
            matches_test = data_test[data_valid.index == data_test.index]
            matches_valid = data_valid[data_valid.index == data_valid.index]
            non_matches = data_test[data_valid.index != data_test.index]
            # Compare rows where primary key is the same:
            for row in range(0, len(matches_test)):
                test = matches_test.iloc[row]
                valid = matches_valid.iloc[row]
                rowDiff = { col : None for col in diff }
                appendDiff = False
                for col in matches_test.columns:
                    if test[col] != valid[col]:
                        rowDiff[col] = '%s vs %s' % (test[col], valid[col])
                        appendDiff = True
                # Append differing values if differences occurred:
                if appendDiff:
                    for num, col in enumerate(matches_test.index.names):
                        rowDiff[col] = matches_test.index[row][num]
                    for col in rowDiff:
                        diff[col].append(rowDiff[col])
            # Write entire row for each data_test pkey value not present in data_valid:
            for row in range(0, len(non_matches)):
                target = non_matches.iloc[row]
                for col in diff:
                    diff[col].append(data_test.iloc[row][col])    
            return DataFrame(diff), missingColsMsg
        else:
            # Compare rows as they appear in descending order:
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
        if not ignoreCols is None and not hasattr(ignoreCols, '__iter__'):
            errs.append('ignoreCols must be an iterable of strings if provided.')
        elif not ignoreCols is None and any([not isinstance(val, str) for val in ignoreCols]):
            errs.append('ignoreCols must be an iterable of strings if provided.')
        if not pKey is None and not (isinstance(pKey, str) or hasattr(pKey, '__iter__')):
            errs.append('pKey must be a string/iterable of strings if provided.')
        elif isinstance(pKey, str):
            if hasattr(ignoreCols, '__iter__') and pKey in ignoreCols:
                errs.append('pKey cannot be in ignoreCols.')
            if isinstance(data_test, DataFrame) and not pKey.lower() in [col.lower() for col in data_test.columns]:
                errs.append('pKey not present in data_test.')
            if isinstance(data_valid, DataFrame) and not pKey.lower() in [col.lower() for col in data_valid.columns]:
                errs.append('pKey not present in data_valid.')
        elif hasattr(pKey, '__iter__'):
            if any([not isinstance(key, str) for key in pKey]):
                errs.append('pKey must only contain strings if an iterable.')
            else:
                if hasattr(ignoreCols, '__iter__'):
                    overlap = set(pKey).intersection(set(ignoreCols))
                    if overlap:
                        errs.append('The following pKey columns overlap with ignoreCols: %s' % overlap)
                if isinstance(data_test, DataFrame):
                    missing = set([key.lower() for key in pKey]) - set([col.lower() for col in data_test.columns])
                    if missing:
                        errs.append('The following pkeys are missing from data_test: %s' % ','.join(missing))
                elif isinstance(data_valid, DataFrame):
                    missing = set([key.lower() for key in pKey]) - set([col.lower() for col in data_valid.columns])
                    if missing:
                        errs.append('The following pkeys are missing from data_valid: %s' % ','.join(missing))
        if errs:
            raise Exception('\n'.join(errs))