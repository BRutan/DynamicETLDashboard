#####################################
# DataColumnAttributes.py
#####################################
# Description:
# * Aggregate column attributes (attributes and relationships).

from Columns.ColumnAttributes import ColumnAttributes
from Columns.ColumnRelationships import ColumnRelationships
import copy
import csv
from datetime import datetime, date
import dateutil.parser as dtparse
from operator import neg
import os
from pandas import DataFrame, concat
import re
from sortedcontainers import SortedDict
import string
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
    __etl_excl_chars = set([ch for ch in string.punctuation])
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
        self.__filepaths = set()
    ###################
    # Properties:
    ###################
    @property
    def FilePaths(self):
        return copy.deepcopy(self.__filepaths)
    @property
    def HasErrors(self):
        return len(self.__errors) > 0
    @property
    def Sheets(self):
        return copy.deepcopy(self.__sheets)
    ###################
    # Interface Methods:
    ###################
    def GetDataAttributes(self, path, dateFormat, fileExp = None, filePaths = None, sheets = None, delim = None):
        """
        * Get all column attributes in files at path or at provided paths.
        Inputs:
        * path: String to folder.
        * dateFormat: Regex string for file dates.
        Optional:
        * fileExp: Regular expressions to select files or None. If not supplied then all files in folder will
        be chosen.
        * filePaths: Dictionary mapping { FileName -> Path }.
        * sheets: Sheets to use if using xls/xlsx file (will create one ETL/table definition per sheet).
        * delim: String delimiter used in csv file.
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
        if not sheets is None and not isinstance(sheets, list):
            errs.append('sheets must be a list if provided.')
        if errs:
            raise Exception("\n".join(errs))
        self.__hasuniques = { sheet : False for sheet in sheets } if not sheets is None else self.__hasuniques
        self.__sheets = sheets
        self.__dateFormat = dateFormat
        # Get all files that match data file expression at provided path if not supplied:
        if filePaths is None:
            filePaths = FileConverter.GetAllFilePaths(path, fileExp)
            if len(filePaths) == 0:
                raise Exception('Could not find any matching files matching regex.')
        # Get column attributes of all target files:
        for file in filePaths:
            path = filePaths[file]
            if self.__sheets is None:
                self.__ExtractFile(path)
            else:
                self.__ExtractAllSheets(path)
        self.__filepaths = set([filePaths[key] for key in filePaths])
        # Determine if columns have changed:
        prevAttrs = None
        if len(self.__dateToAttrs) > 1 and self.__sheets is None:
            for dt in self.__dateToAttrs:
                currAttrs = self.__dateToAttrs[dt]
                if not prevAttrs is None and currAttrs != prevAttrs:
                    self.__columnChgDates[currAttrs.FileDate] = currAttrs - prevAttrs
                prevAttrs = currAttrs
        elif len(self.__dateToAttrs) > 1 and not self.__sheets is None:
            # Determine if columns have changed for each sheet:
            for dt in self.__dateToAttrs:
                prevAttrs = None
                for sheetname in self.__dateToAttrs[dt]:
                    currAttrs = self.__dateToAttrs[dt][sheetname]
                    if not prevAttrs is None and currAttrs != prevAttrs:
                        self.__columnChgDates[currAttrs.FileDate][sheetname] = currAttrs - prevAttrs
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
        # Generate one report per sheet in workbook if
        # data workbooks consist of multiple sheets:
        if not self.__sheets is None:
            self.__GenerateAllReports(path)
        else:
            wb = xlsxwriter.Workbook(path)
            self.__GenColumnAttributeSheet(wb)
            self.__GenColChangeDateSheet(wb)
            self.__GenColRelationshipsSheet(wb)
            self.__GenUniquesSheet(wb)
            wb.close()
        
    def CreateTableDefinitions(self, outputpath, table = None, allnull = False):
        """
        * Create SQL table definition based upon latest 
        columnattributes.
        Will create one table definition per sheet if file consists of 
        multiple sheets.
        Inputs:
        * outputpath: Folder to output one or more .sql files.
        * table: Name for table definition .sql file.
        """
        if not self.__dateToAttrs:
            return
        errs = []
        if not table is None and not isinstance(table, str):
            errs.append('table must be None or a string.')
        if not os.path.isdir(outputpath):
            errs.append('outputpath must be a folder')
        if errs:
            raise Exception('\n'.join(errs))
        outputpath += ('\\' if not outputpath.endswith('\\') else '')
        latest = max(self.__dateToAttrs)
        table = table.replace(' ', '') if not table is None else 'Tabledef'
        if not self.__sheets is None:
            # Create one table definition per sheet:
            for sheetname in self.__dateToAttrs[latest]:
                attr = self.__dateToAttrs[latest][sheetname]
                table_full = ('%s.%s' %  (table, sheetname))
                table_full = DataColumnAttributes.__FixName(table_full)
                path = '%s%s.sql' % (outputpath, table_full)
                with open(path, 'w') as f:
                    self.__WriteTableDef(f, attr, table_full, allnull)
        else:
            attr = self.__dateToAttrs[latest]
            tablename = table.replace(' ', '')
            path = ('%s%s.sql' % (outputpath, tablename))
            with open(path, 'w') as f:
                self.__WriteTableDef(f, attr, table, allnull)

    ##################
    # Private Helpers:
    ################## 
    def __ExtractFile(self, path):
        """
        * Extract data from single file.
        """
        currAttrs = ColumnAttributes(path, self.__dateFormat)
        if currAttrs.Error:
            tail, file = os.path.split(path)
            self.__errors[file] = currAttrs.Error
        else:
            self.__hasuniques = self.__hasuniques if self.__hasuniques else any([True for col in currAttrs.Attributes if not currAttrs.Attributes[col] is None])
            # Map { FileDate -> ColumnAttributes }:
            self.__dateToAttrs[currAttrs.FileDate] = currAttrs

    def __ExtractAllSheets(self, path):
        """
        * Extract column attributes from multiple target sheets, to each be implemented
        as own ETLs.
        """
        filedate = self.__GetFileDate(path)
        self.__dateToAttrs[filedate] = SortedDict()
        for sheetname in self.__sheets:
            currAttrs = ColumnAttributes(path, self.__dateFormat, sheetname)
            if currAttrs.Error:
                self.__errors[file][sheetname] = currAttrs.Error
            else:
                self.__hasuniques[sheetname] = self.__hasuniques[sheetname] if self.__hasuniques[sheetname] else any([True for col in currAttrs.Attributes if not currAttrs.Attributes[col] is None])
                # Map { FileDate -> { SheetName -> ColumnAttributes }}:
                self.__dateToAttrs[filedate][sheetname] = currAttrs

    def __GenerateAllReports(self, path):
        """
        * Generate one report per target sheet.
        """
        subpath = path[0:path.rfind('.')]
        for sheetname in self.__sheets:
            sheetname_fixed = DataColumnAttributes.__FixName(sheetname)
            outpath = subpath + '_' + sheetname_fixed + '.xlsx'
            wb = xlsxwriter.Workbook(outpath)
            self.__GenColumnAttributeSheet(wb, sheetname)
            self.__GenColChangeDateSheet(wb, sheetname)
            self.__GenColRelationshipsSheet(wb, sheetname)
            self.__GenUniquesSheet(wb, sheetname)
            wb.close()
        
    def __GetFileDate(self, path):
        """
        * Extract file date from file name.
        """
        format = { arg.lower() : self.__dateFormat[arg] for arg in self.__dateFormat }
        match = format['regex'].search(path)[0]
        return datetime.strptime(match, format['dateformat'])

    def __GenColumnAttributeSheet(self, wb, sheetname = None):
        """
        * Create Column Report sheet that details column attributes for latest file.
        """
        colReport = wb.add_worksheet('Column Attributes')
        headerFormat = wb.add_format(DataColumnAttributes.__headerFormat)
        headers = DataColumnAttributes.__columnReportHeaders
        latest = max(self.__dateToAttrs)
        latest = self.__dateToAttrs[latest] if sheetname is None else self.__dateToAttrs[latest][sheetname]
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
        
    def __GenColChangeDateSheet(self, wb, sheetname = None):
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
            attr = self.__columnChgDates[dt] if sheetname is None else self.__columnChgDates[dt][sheetname]
            row = attr.ToReportRow()
            for num, val in enumerate(row):
                chgSheet.write(rowNum, num, val)
            rowNum += 1

    def __GenColRelationshipsSheet(self, wb, sheetname = None):
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
            attr = self.__dateToAttrs[dt] if sheetname is None else self.__dateToAttrs[dt][sheetname]
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
                            relSheet.write(filerow, num, "", type_format[df[col][row - 1]])
                        else:
                            # Write row index:
                            relSheet.write(filerow, num, df[cols[1]].index[row - 1], headerFormat)
                    else:
                        # Write column headers:
                        relSheet.write(filerow, num, col, headerFormat)
            rowOff += df.shape[0] + 3

    def __GenUniquesSheet(self, wb, sheetname = None):
        """
        * Add Uniques sheet listing all unique values for columns
        that have a uniquecount below threshhold.
        """
        if not self.__hasuniques if sheetname is None else not self.__hasuniques[sheetname]:
            return
        uniqueSht = wb.add_worksheet('Uniques')
        headerFormat = wb.add_format(DataColumnAttributes.__headerFormat)
        rowOff = 0
        for dt in self.__dateToAttrs:
            attrs = self.__dateToAttrs[dt] if sheetname is None else self.__dateToAttrs[dt][sheetname]
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

    def __WriteTableDef(self, file, attributes, table = None, allnull = False):
        """
        * Write table definition in standard format.
        """
        if not table is None:
            table_fixed = DataColumnAttributes.__FixName(table)
        else:
            table_fixed = '<FillTableHere>'
        file.write('USE [MetricsDyetl];\nGO\n\n')
        file.write('SET ANSI_NULLS ON;\nGO\n\n')
        file.write('SET QUOTED_IDENTIFIER ON;\nGO\n\n')
        file.write('/****** Object: Table [dbo].[%s] Script Date: %s ******/\n\n' % (table_fixed, datetime.today().strftime('%m %d %Y %H:%M:%S %p')))
        file.write('CREATE TABLE [dbo].[%s]\n' % table_fixed)
        file.write('(\n')
        for num, name in enumerate(attributes.Attributes):
            attr = attributes.Attributes[name]
            if allnull:
                nullable = 'NULL'
            else:
                nullable = 'NOT NULL' if not attr.IsNullable else 'NULL'
            file.write('[%s] %s %s,\n' % (attr.ColumnName, attr.Type, nullable))
        file.write('[FileDate] datetime NOT NULL,\n')
        file.write('[RunDate] datetime NOT NULL\n')
        file.write(') ON [PRIMARY];\n\nGO\n')

    @classmethod
    def __FixName(cls, name):
        startIndex = name.rfind('.') + 1
        endIndex = len(name)
        subname = name[startIndex:endIndex]
        subname_fixed = ''.join([ch if ch not in DataColumnAttributes.__etl_excl_chars else ' ' for ch in subname])
        subname_fixed = subname_fixed.title().replace(' ', '') if len(subname_fixed.replace(' ', '')) != len(subname_fixed) else subname_fixed
        name_fixed = name[0:startIndex] + subname_fixed

        return name_fixed