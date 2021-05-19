#####################################
# CompareGroupedData.py
#####################################
# Description:
# * Compare all column attributes within 
# all datasets meant to be a part
# of one centralized datasets.

from pandas import DataFrame, read_excel, read_csv, Series

# Code to be implemented in objects:

def generate_columnreport(tickerToCol, execution_date):
    """
    * Generate matrix detailing which funds
    have same columns and order.
    """
    output = {'Fund' : []}
    for lticker in tickerToCol:
        lcols = tickerToCol[lticker]
        output['Fund'].append(lticker)
        for rticker in tickerToCol:
            if not rticker in output:
                output[rticker] = []
            if rticker == lticker:
                output[rticker].append(1)
            else:
                rcols = tickerToCol[rticker]
                if all([lcols[num] == col for num, col in enumerate(rcols)]):
                    output[rticker].append(1)
                else:
                    output[rticker].append(0)

    pd.DataFrame(output).set_index(['Fund']).to_csv('uniquecol_matrix_%s.csv' % execution_date.strftime('%m%d%Y'))

def generate_uniquecolreport(tickerToCol, execution_date):
    """
    * Generate column report.
    """
    all_cols = set()
    for ticker in tickerToCol:
        all_cols.update(tickerToCol[ticker])
    output = {'Column' : list(all_cols)}
    for ticker in tickerToCol:
        tickerCols = tickerToCol[ticker]
        output[ticker] = []
        for col in all_cols:
            if col in tickerCols:
                output[ticker].append(1)
            else:
                output[ticker].append(0)
    pd.DataFrame(output).set_index(['Column']).to_csv('fund_to_uniquecols_%s.csv' % execution_date.strftime('%m%d%Y'))




def main():
    """
    * Perform key steps in order.
    """
    args = get_args()
    files = get_files(args)
    compare_data(files, args)

def get_args():
    """
    * Pull arguments from json file.
    """
    pass

def get_files(args):
    """
    * Pull all files meant to be combined into
    one dataset.
    """
    pass

def compare_data(files, args):
    """
    * Generate report detailing which files
    have which columns.
    """
    all_cols = {}
    for file in files:
        pass
    pass





