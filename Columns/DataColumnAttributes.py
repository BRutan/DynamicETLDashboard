#####################################
# DataColumnAttributes.py
#####################################
# Description:
# * Aggregate column attributes (attributes and relationships).

from Columns.ColumnAttributes import ColumnAttributes
from Columns.ColumnRelationships import ColumnRelationships
import csv
from datetime import datetime, date
import dateutil.parser as dtparse
import os
from pandas import DataFrame, concat
import re
from sortedcontainers import SortedDict
import xlsxwriter

class DataColumnAttributes(object):
    """
    * Look through all data files at path, generate report
    for columns.
    """
    __columnReportHeaders = ['Name', 'Type', 'IsNullable', 'IsUnique', 'UniqueCount']
    __columnChgReportHeaders = ['Date', 'Name', 'Type', 'Nullable', 'IsUnique', 'UniqueCount']
    __regType = type(re.compile(''))
    def __init__(self):
        """
        * Instantiate new object.
        """
        # Map date to attributes, and attributes that have changed with time:
        self.__hasuniques = False
        self.__dateToAttrs = SortedDict()
        self.__columnChgDates = SortedDict()
        self.__errors = {}

    def GetDataAttributes(self, path, dateFormat, fileExp = None):
        """
        * Generate report using all files at path. If fileExp is provided
        then only search files matching expression.
        Inputs:
        * path: String to folder.
        * dateFormat: Regex string for file dates.
        * fileExp: Regular expressions to select files or None.
        """
        errs = []
        if fileExp and not isinstance(fileExp, DataColumnAttributes.__regType):
            errs.append("fileExp must be a regular expression object, or None.")
        if not isinstance(path, str):
            errs.append("path must be a string.")
        if not isinstance(dateFormat, dict):
            errs.append("dateFormat must be a dictionary with keys ['regex', 'dateformat' ].")
        elif not 'regex' in dateFormat and 'dateformat' not in dateFormat:
            errs.append("dateFormat must have 'regex' and 'dateformat' keys.")
        if errs:
            raise Exception("\n".join(errs))

        self.__dateFormat = dateFormat
        # Get all files that match data file expression at provided path:
        filePaths = []
        if fileExp:
            filePaths = [os.path.join(path, file) for file in os.listdir(path) if os.path.isfile(os.path.join(path, file)) and fileExp.match(file)]
        else:
            filePaths = [os.path.join(path, file) for file in os.listdir(path) if os.path.isfile(os.path.join(path, file))]

        # Get column attributes of all target files:
        for path in filePaths:
            currAttrs = ColumnAttributes(path, dateFormat)
            if currAttrs.Error:
                self.__errors[path] = currAttrs.Error
            else:
                self.__hasuniques = self.__hasuniques if self.__hasuniques else any([True for col in currAttrs.Attributes if not currAttrs.Attributes[col] is None])
                # Map { FileDate -> ColumnAttributes }:
                self.__dateToAttrs[currAttrs.FileDate] = currAttrs

        # Determine if columns have changed:
        prevAttrs = None
        if len(self.__dateToAttrs) > 1:
            for dt in self.__dateToAttrs:
                currAttrs = self.__dateToAttrs[dt]
                if not prevAttrs is None and currAttrs != prevAttrs:
                    self.__columnChgDates[currAttrs.FileDate] = currAttrs - prevAttrs
                prevAttrs = currAttrs

    def GenerateReport(self, path):
        """
        * Output report to file at path.
        """
        if not isinstance(path, str):
            raise Exception("path must be a string.")
        elif not '.xlsx' in path:
            raise Exception("file must be a .xlsx file.")
        if not self.__dateToAttrs:
            # Skip report if no column attributes could be generated:
            return

        wb = xlsxwriter.Workbook(path)
        # Create Column Report sheet that details column attributes for latest file:
        colReport = wb.add_worksheet('Column Type Report')
        headers = DataColumnAttributes.__columnReportHeaders
        latest = max(self.__dateToAttrs)
        latest = self.__dateToAttrs[latest]
        rowNum = 0
        for header in headers:
            colNum = 0
            colReport.write(rowNum, colNum, header)
            for name in latest.Attributes:
                colNum += 1
                attr = latest.Attributes[name]
                val = attr.ToReportCell(header)
                colReport.write(rowNum, colNum, val)
            rowNum += 1

        # Create Column Change report that details how columns have changed over time:
        if self.__columnChgDates:
            chgSheet = wb.add_worksheet('Column Chg Report')
            headers = DataColumnAttributes.__columnChgReportHeaders
            rowNum = 0
            for num, header in enumerate(headers):
                chgSheet.write(rowNum, num, header)
            for dt in self.__columnChgDates:
                rowNum += 1
                attr = self.__columnChgDates[dt]
                row = attr.ToReportRow()
                for num, val in enumerate(row):
                    chgSheet.write(rowNum, num, val)
                rowNum += 1
        # Add sheet with all column relationships for each file:
        attrSheet = wb.add_worksheet('Column Attributes')
        #attrSheet.write(0, 0, "Format is <ColUniqueNum>_<RowUniqueNum>")
        #rowOff = 1
        rowOff = 0
        for dt in self.__dateToAttrs:
            attr = self.__dateToAttrs[dt]
            df = attr.Relationships.ToDataFrame(False)
            # Write file date:
            attrSheet.write(rowOff, 0, "File Date")
            attrSheet.write(rowOff, 1, dt.strftime('%m/%d/%Y'))
            # Write columns:
            cols = list(df.columns.copy())
            cols.insert(0, '')
            for row in range(0, len(df) + 1):
                filerow = row + rowOff + 1
                for num, col in enumerate(cols):
                    if row != 0:
                        if num != 0:
                            attrSheet.write(filerow, num, df[col][row - 1])
                        else:
                            # Write row index:
                            attrSheet.write(filerow, num, df[cols[1]].index[row - 1])
                    else:
                        # Write column headers:
                        attrSheet.write(filerow, num, col)
            rowOff += df.shape[0] + 1

        # Add Uniques sheet listing all unique values for columns:
        if self.__hasuniques:
            uniqueSht = wb.add_worksheet('Uniques')
            rowOff = 0
            for dt in self.__dateToAttrs:
                attrs = self.__dateToAttrs[dt]
                uniqueCols = [col for col in attrs.Attributes if not attrs.Attributes[col].Uniques is None] if attrs.Attributes else None
                if not uniqueCols:
                    continue
                maxUniques = max([len(attrs.Attributes[col].Uniques) for col in uniqueCols])
                attrSheet.write(rowOff, 0, "File Date")
                attrSheet.write(rowOff, 1, dt.strftime('%m/%d/%Y'))
                for colNum, col in enumerate(uniqueCols):
                    attr = attrs.Attributes[col]
                    for row in range(0, len(attr.Uniques) + 1):
                        filerow = row + rowOff
                        if row != 0:
                            # Write data:
                            uniqueSht.write(filerow, colNum, attr.Uniques[row - 1])
                        else:
                            # Print column name:
                            uniqueSht.write(filerow, colNum, col)
                rowOff += maxUniques + 1

        wb.close()
        
    def CreateTableDefinition(self, table = None):
        """
        * Create SQL table definition based upon latest 
        columnattributes.
        """
        if not table is None and not isinstance(table, str):
            raise Exception('table must be None or a string.')
        latest = max(self.__dateToAttrs)
        latest = self.__dateToAttrs[latest]
        path = '%s.sql' % table if not table is None else 'tabledef'
        with open(path, 'w') as f:
            self.__WriteTableDef(f, latest, table)

    ##################
    # Private Helpers:
    ##################
    def __WriteTableDef(self, file, attributes, table = None):
        """
        * Write table definition in standard format.
        """
        file.write('USE [MetricsDyetl];\nGO\nSET ANSI_NULLS ON\nGO\n\n')
        file.write('SET ANSI_NULLS ON;\nGO\n\n')
        file.write('SET QUOTED_IDENTIFIER ON;\nGO\n\n')
        file.write('//****** Object: Table [dbo].[%s] Script Date: %s ******//\n\n' % (table if not table is None else 'FillTableHere', datetime.today().strftime('%m %d %Y %H:%M:%S %p')))
        file.write('CREATE TABLE [dbo].[%s]\n' % table if not table is None else 'FillTableHere')
        file.write('(')
        for num, name in enumerate(attributes.Attributes):
            attr = attributes.Attributes[name]
            nullable = 'NOT NULL' if not attr.IsNullable else 'NULL'
            last = num == len(attributes.Attributes) - 1
            file.write('[%s] %s %s%s\n' % (attr.ColumnName, attr.Type, nullable, ',' if last else ''))
        file.write(') ON [PRIMARY];\n\nGO\n')
