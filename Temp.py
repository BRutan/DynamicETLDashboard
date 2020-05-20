
from ETL.TSQLInterface import TSQLInterface

def temp():
    #interface = TSQLInterface('nj1sql13', 'MetricsDyetl')
    query = 'SELECT * FROM [dbo].[tbl_CyberSecurity_Exceptions_GS]'
    data_valid = interface.Select(query)
    interface = TSQLInterface('.', 'MetricsDyetl')

    # Compare data in MetricsDB versus MetricsDYETL data:
    interface


def temp2():
    interface = TSQLInterface('.', 'MetricsDyetl')
    query = 'SELECT * FROM [MetricsDyetl].[dbo].[tbl_CyberSecurity_Exceptions_GS]'
    interface.Select(query)

if __name__ == '__main__':
    temp()