
from ETL.DataReader import DataReader
from ETL.DataComparer import DataComparer
from ETL.TSQLInterface import TSQLInterface

def compare():
    interface = TSQLInterface('nj1uatsql13', 'Metrics')
    query = 'SELECT * FROM [dbo].[tbl_CyberSecurity_Exceptions_GS]'
    data_valid = interface.Select(query)
    interface = TSQLInterface('.', 'MetricsDyetl')
    data_test = interface.Select(query)
    ignoreCols = ['FileDate', 'RunDate']
    pKey = 'ID'
    DataComparer.GenerateComparisonReport('CyberSecurityExceptionsDiff.xlsx', data_test, data_valid, ignoreCols, pKey)


def getdata():
    interface = TSQLInterface('nj1uatsql13', 'Metrics')
    query = 'SELECT * FROM [dbo].[tbl_CyberSecurity_Exceptions_GS]'
    data_valid = interface.Select(query)
    interface = TSQLInterface('.', 'MetricsDyetl')
    interface.Insert(data_valid, 'tbl_CyberSecurity_Exceptions_GS', True)


if __name__ == '__main__':
    compare()