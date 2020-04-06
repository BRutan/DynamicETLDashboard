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
from operator import neg
import os
from pandas import DataFrame, concat
import re
from sortedcontainers import SortedDict
from Utilities.FileConverter import FileConverter
import xlsxwriter

class DataColumnAttributes(object):
    """
    * Look through all data files at path, generate report
    for columns.
    """
    __columnReportHeaders = ['Name', 'Type', 'IsNullable', 'IsUnique', 'UniqueCount']
    __columnChgReportHeaders = ['Date', 'Name', 'Type', 'Nullable', 'IsUnique', 'UniqueCount']
    # For report generation:
    __headerFormat = {'bold': True, 'font_color': 'white', 'bg_color' : 'black'}
    __one_one_format = {'font_color': 'black', 'bg_color' : 'green'}
    __one_many_format = {'font_color': 'black', 'bg_color' : 'blue'}
    __many_many_format = {'font_color': 'black', 'bg_color' : 'red'}
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
    ###################
    # Properties:
    ###################
    @property
    def HasErrors(self):
        return len(self.__errors) > 0
    ###################
    # Interface Methods:
    ###################
    def GetDataAttributes(self, path, dateFormat, fileExp = None, filePaths = None):
        """
        * Get all column attributes in files at path or at provided paths.
        Inputs:
        * path: String to folder.
        * dateFormat: Regex string for file dates.
        Optional:
        * fileExp: Regular expressions to select files or None. If not supplied then all files in folder will
        be chosen.
        * filePaths: Dictionary mapping { FileName -> Path }.
        """
        errs = []
        if not isinstance(path, str):
            errs.append("path must be a string.")
        if not isinstance(dateFormat, dict):
            errs.append("dateFormat must be a dictionary with keys ['regex', 'dateformat' ].")
        elif not 'regex' in dateFormat and 'dateformat' not in dateFormat:
            errs.append("dateFormat must have 'regex' and 'dateformat' keys.")
        if fileExp and not isinstance(fileExp, DataColumnAttributes.__regType):
            errs.append("fileExp must be a regular expression object, or None.")
        if filePaths and not isinstance(filePaths, dict):
            errs.append('filePaths must be a dictionary mapping { FileName -> Path } or None.')
        if errs:
            raise Exception("\n".join(errs))

        self.__dateFormat = dateFormat
        # Get all files that match data file expression at provided path if not supplied:
        if filePaths is None:
            filePaths = FileConverter.GetAllFilePaths(path, fileExp)
        # Get column attributes of all target files:
        for file in filePaths:
            path = filePaths[file]
            currAttrs = ColumnAttributes(path, dateFormat)
            if currAttrs.Error:
                self.__errors[file] = currAttrs.Error
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
        self.__GenColumnAttributeSheet(wb)
        self.__GenColChangeDateSheet(wb)
        self.__GenColRelationshipsSheet(wb)
        self.__GenUniquesSheet(wb)
        wb.close()
        
    def CreateTableDefinition(self, table = None):
        """
        * Create SQL table definition based upon latest 
        columnattributes.
        """
        if not table is None and not isinstance(table, str):
            raise Exception('table must be None or a string.')
        if not self.__dateToAttrs:
            return
        latest = max(self.__dateToAttrs)
        latest = self.__dateToAttrs[latest]
        path = '%s.sql' % table if not table is None else 'tabledef'
        with open(path, 'w') as f:
            self.__WriteTableDef(f, latest, table)

    ##################
    # Private Helpers:
    ##################
    def __GenColumnAttributeSheet(self, wb):
        """
        * Create Column Report sheet that details column attributes for latest file.
        """
        colReport = wb.add_worksheet('Column Attributes')
        headerFormat = wb.add_format(DataColumnAttributes.__headerFormat)
        headers = DataColumnAttributes.__columnReportHeaders
        latest = max(self.__dateToAttrs)
        latest = self.__dateToAttrs[latest]
        rowNum = 0
        for header in headers:
            colNum = 0
            colReport.write(rowNum, colNum, header, headerFormat)
            for name in latest.Attributes:
                colNum += 1
                attr = latest.Attributes[name]
                val = attr.ToReportCell(header)
                colReport.write(rowNum, colNum, val)
            rowNum += 1
        
    def __GenColChangeDateSheet(self, wb):
        """
        * Create Column Change sheet that details how columns have changed over time.
        """
        if not self.__columnChgDates:
            return
        chgSheet = wb.add_worksheet('Column Chg Report')
        headerFormat = wb.add_format(DataColumnAttributes.__headerFormat)
        headers = DataColumnAttributes.__columnChgReportHeaders
        rowNum = 0
        for num, header in enumerate(headers):
            chgSheet.write(rowNum, num, header, headerFormat)
        for dt in self.__columnChgDates:
            rowNum += 1
            attr = self.__columnChgDates[dt]
            row = attr.ToReportRow()
            for num, val in enumerate(row):
                chgSheet.write(rowNum, num, val)
            rowNum += 1

    def __GenColRelationshipsSheet(self, wb):
        """
        * Add sheet with all column relationships for each file.
        """
        relSheet = wb.add_worksheet('Column Relationships')
        headerFormat = wb.add_format(DataColumnAttributes.__headerFormat)
        one_one = wb.add_format(DataColumnAttributes.__one_one_format) 
        one_many = wb.add_format(DataColumnAttributes.__one_many_format)
        many_many = wb.add_format(DataColumnAttributes.__many_many_format)
        type_format = SortedDict({'one_one' : one_one, 'one_many' : one_many, 
                       'many_one' : one_many, 'many_many' : many_many, 
                       '=' : wb.add_format({'font_color': 'black', 'bg_color' : 'white'})})
        # Write the color key:
        relSheet.write(0, 0, 'Color Key:', headerFormat)
        col = 1
        for type_ in type_format:
            if type_ != '=':
                relSheet.write(0, col, type_, headerFormat)
                relSheet.write(1, col, '', type_format[type_])
                col += 1
        rowOff = 2
        # Write all relationships for each report:
        for dt in self.__dateToAttrs:
            attr = self.__dateToAttrs[dt]
            df = attr.Relationships.ToDataFrame(False)
            # Write file date:
            relSheet.write(rowOff, 0, "File Date", headerFormat)
            relSheet.write(rowOff, 1, dt.strftime('%m/%d/%Y'))
            # Write columns:
            cols = list(df.columns.copy())
            cols.insert(0, '')
            for row in range(0, len(df) + 1):
                filerow = row + rowOff + 1
                for num, col in enumerate(cols):
                    if row != 0:
                        if num != 0:
                            relSheet.write(filerow, num, df[col][row - 1], type_format[df[col][row - 1]])
                        else:
                            # Write row index:
                            relSheet.write(filerow, num, df[cols[1]].index[row - 1], headerFormat)
                    else:
                        # Write column headers:
                        relSheet.write(filerow, num, col, headerFormat)
            rowOff += df.shape[0] + 3

    def __GenUniquesSheet(self, wb):
        """
        * Add Uniques sheet listing all unique values for columns
        that have a uniquecount below threshhold.
        """
        if not self.__hasuniques:
            return
        uniqueSht = wb.add_worksheet('Uniques')
        headerFormat = wb.add_format(DataColumnAttributes.__headerFormat)
        rowOff = 0
        for dt in self.__dateToAttrs:
            attrs = self.__dateToAttrs[dt]
            uniqueCols = [col for col in attrs.Attributes if not attrs.Attributes[col].Uniques is None] if attrs.Attributes else None
            if not uniqueCols:
                continue
            maxUniques = max([len(attrs.Attributes[col].Uniques) for col in uniqueCols])
            # Write file date:
            uniqueSht.write(rowOff, 0, "File Date", headerFormat)
            uniqueSht.write(rowOff, 1, dt.strftime('%m/%d/%Y'))
            rowOff += 1
            for colNum, col in enumerate(uniqueCols):
                attr = attrs.Attributes[col]
                for row in range(0, len(attr.Uniques) + 1):
                    filerow = row + rowOff
                    if row != 0:
                        # Write data:
                        uniqueSht.write(filerow, colNum, str(attr.Uniques.iloc[row - 1]))
                    else:
                        # Print column name:
                        uniqueSht.write(filerow, colNum, col, headerFormat)
            rowOff += maxUniques + 3

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
