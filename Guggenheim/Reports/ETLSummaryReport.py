#####################################
# ETLSummaryReport.py
#####################################
# Description:
# * Class that creates specific summary reports
# for ETLs detailing result after ETL has completed following DynamicEtl.Service
# run in production.

from datetime import datetime
from ETL.DataReader import DataReader
from ETL.TSQLInterface import TSQLInterface
import os
import xlsxwriter

class ETLSummaryReport:
    """
    * Class that creates specific summary reports
    for ETLs detailing result after ETL has completed following DynamicEtl.Service
    run in production.
    """
    __headerFormat = {'bold': True, 'font_color': 'white', 'bg_color' : 'black'}
    __optional = set(['pre', 'input', 'post', 'delim'])
    __req = set(['etl', 'inputfilepath', 'filedate', 'tablename', 'database', 'server', 'status', 'starttime', 'endtime'])
    def __init__(self, **kwargs):
        """
        * Accumulate data necessary to generate report 
        using passed arguments.
        """
        # Normalize arguments:
        kwargs = { arg.lower() : kwargs[arg] for arg in kwargs if isinstance(arg, str) }
        ETLSummaryReport.__Validate(**kwargs)
        self.__GetProperties(**kwargs)
        self.__AccumulateAttributes(**kwargs)

    #################
    # Properties:
    #################
    @property
    def Database(self):
        return self.__database
    @property
    def EndTime(self):
        return self.__endtime
    @property
    def ETLName(self):
        return self.__etlname
    @property
    def ETLStatus(self):
        return self.__etlstatus
    @property
    def FileRowCount(self):
        return self.__filerowcount
    @property
    def InputOperations(self):
        return self.__inputoperations
    @property
    def InsertionRowCount(self):
        return self.__insertionrowcount
    @property
    def PostOperations(self):
        return self.__postoperations
    @property
    def PreOperations(self):
        return self.__preoperations
    @property
    def Server(self):
        return self.__server
    @property
    def StartTime(self):
        return self.__starttime
    @property
    def TableName(self):
        return self.__tablename

    #################
    # Interface Methods:
    #################
    def GenerateReport(self, reportpath):
        """
        * Generate ETLSummaryReport at path.
        """
        if not isinstance(reportpath, str):
            raise Exception('reportpath must be a string.')
        elif not reportpath.endswith('xlsx'):
            raise Exception('reportpath must point to .xlsx file.')
        else:
            pass
        # Generate report:
        wb = xlsxwriter.Workbook(reportpath)
        self.__FillData(wb)
        wb.close()

    #################
    # Private Helpers:
    #################
    def __FillData(self, wb):
        """
        * Fill data into the workbook.
        """
        headerFormat = wb.add_format(ETLSummaryReport.__headerFormat)
        summarySheet = wb.add_worksheet('Summary')
        # chgSheet.write(rowNum, 0, "FileDate:", headerFormat)
        properties = [attr for attr in dir(self) if not attr.startswith('_') and not callable(getattr(self, attr))]
        for row, prop in enumerate(properties):
            summarySheet.write(row, 0, prop, headerFormat)
            obj = getattr(self, prop)
            if isinstance(obj, datetime):
                val = obj.strftime('%m/%d/%Y %H:%M:%S')
            elif not isinstance(obj, str) and hasattr(obj, '__iter__'):
                val = ';'.join(obj)
            else:
                val = obj
            summarySheet.write(row, 1, val)

    def __GetProperties(self, **kwargs):
        """
        * Accumulate all properties from kwargs 
        before analyzing.
        """
        self.__database = kwargs['database']
        self.__endtime = kwargs['endtime']
        self.__etlname = kwargs['etl']
        self.__etlstatus = kwargs['status']
        self.__filerowcount = 0
        self.__inputoperations = list(kwargs['input'] if 'input' in kwargs and not kwargs['input'] is None else list())
        self.__insertionrowcount = 0
        self.__postoperations = list(kwargs['post'] if 'post' in kwargs and not kwargs['post'] is None else list())
        self.__preoperations = list(kwargs['pre'] if 'pre' in kwargs and not kwargs['pre'] is None else list())
        self.__server = kwargs['server']
        self.__starttime = kwargs['starttime']
        self.__tablename = kwargs['tablename']

    def __AccumulateAttributes(self, **kwargs):
        """
        * Accumulate necessary attributes to generate report.
        """
        self.__CompareSources(**kwargs)

    def __CompareSources(self, **kwargs):
        """
        * Compare the input file versus the data that was
        loaded into the table.
        """
        # NOTE: Potentially use REST API with DYEL.Service to get data as opposed
        # to reading original file.
        # Pull original data from file:
        errs = []
        try:
            inputData = DataReader.Read(kwargs['inputfilepath'], delim = kwargs['delim'])
            self.__filerowcount = len(inputData)
        except Exception as ex:
            folder, filename = os.path.split(kwargs['inputfilepath'])
            errs.append('Could not pull data from %s. Reason: %s.' % (filename, str(ex)))
        # Query server to get inserted row count:
        try:
            interface = TSQLInterface(self.__server, self.__database)
            fdStr = kwargs['filedate'].strftime('%m-%d-%Y')
            data = interface.Select("SELECT * FROM [%s] WHERE [FileDate] = '%s'" % (self.__tablename, fdStr))
            self.__insertionrowcount = len(data)
        except Exception as ex:
            errs.append('Could not query %s::%s. Reason: %s' % str(ex))
        if errs:
            raise Exception('\n'.join(errs))

    @staticmethod
    def __Validate(**kwargs):
        """
        * Validate constructor parameters.
        """
        errs = []
        # Ensure all required arguments present:
        missing = ETLSummaryReport.__req - set(kwargs)
        if missing:
            Exception('The following required arguments are missing: %s' % ','.join(missing))
        # Validate required arguments:
        if not isinstance(kwargs['etl'], str):
            errs.append('etl must be a string.')
        if not isinstance(kwargs['inputfilepath'], str):
            errs.append('inputfilepath must be a string.')
        elif not os.path.exists(kwargs['inputfilepath']):
            errs.append('inputfilepath does not exist.')
        if not isinstance(kwargs['filedate'], datetime):
            errs.append('filedate must be a datetime object.')
        if not isinstance(kwargs['tablename'], str):
            errs.append('tablename must be a string.')
        if not isinstance(kwargs['database'], str):
            errs.append('database must be a string.')
        if not isinstance(kwargs['server'], str):
            errs.append('server must be a string.')
        if not isinstance(kwargs['status'], str):
            errs.append('status must be a string.')
        if not isinstance(kwargs['starttime'], datetime):
            errs.append('starttime must be a datetime object.')
        if not isinstance(kwargs['endtime'], datetime):
            errs.append('endtime must be a datetime object.')
        # Validate optional arguments:
        if 'delim' in kwargs and not isinstance(kwargs['delim'], str):
            errs.append('delim must be a string if provided.')
        elif not 'delim' in kwargs:
            kwargs['delim'] = None
        for op in ETLSummaryReport.__optional:
            if op == 'delim':
                continue
            if op in kwargs and not kwargs[op] is None and not (not isinstance(kwargs[op], str) and hasattr(kwargs[op], '__iter__')):
                errs.append('%s must be a container.' % op)
        if errs:
            raise Exception('\n'.join(errs))
