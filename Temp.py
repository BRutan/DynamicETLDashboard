
from ETL.DataReader import DataReader
from ETL.DataComparer import DataComparer
from ETL.TSQLInterface import TSQLInterface

def compare():
    interface = TSQLInterface('nj1uatsql13', 'Metrics')
    query = ['select * ']
    query.append("from tbl_CyberSecurity_Exceptions_GS ")
    #query.append(" where fileDate = '2020-4-13'")
    #query.append(" order by [Employee ID] asc, [Group] asc")
    query = ''.join(query)
    data_valid = interface.Select(query)
    pKey = TSQLInterface.PrimaryKeys(data_valid, maxCombs = 3, ignoreCols = ['ID'], findFirst = True)
    interface = TSQLInterface('.', 'MetricsDyetl')
    data_test = interface.Select(query)
    ignoreCols = ['ID', 'FileDate', 'RunDate']
    DataComparer.GenerateComparisonReport('CyberSecurityExceptionsDiff_Pre.xlsx', data_test, data_valid, ignoreCols, pKey)


def getdata():
    interface = TSQLInterface('nj1uatsql13', 'MetricsDyetl')
    query = 'SELECT * FROM [dbo].[tbl_CyberSecurity_Exceptions_GS]'
    data = interface.Select(query)
    interface = TSQLInterface('.', 'MetricsDyetl')
    interface.Insert(data, 'tbl_CyberSecurity_Exceptions_GS', True)


if __name__ == '__main__':
    compare()