import os, sys
import csv
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import argparse
import bbgClient

FIELDS_LIST = ["PE_RATIO",
               "EQY_DVD_YLD_12M",
               "RETURN_ON_ASSET",
               "INDX_GENERAL_EARN",
               "BS_TOT_ASSET",
               "TRAIL_12M_PROF_MARGIN",
               "TOT_DEBT_TO_TOT_ASSET",
               "DVD_PAYOUT_RATIO",
               "PX_TO_CASH_FLOW",
               "FREE_CASH_FLOW_YIELD",
               "TRAIL_12M_FREE_CASH_FLOW_PER_SH",
               "BOOK_VAL_PER_SH",
               "PX_TO_BOOK_RATIO",
               "TOTAL_DEBT_TO_EV",
               "TRAIL_12M_SALES_PER_SH",
               "BEST_EPS",
               "PX_TO_EBITDA"]

def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-I', '--inputDir', dest='inputDir', default="C:\DataBatch_ETF_NewProject\Inputs", help='Output Directory')
    parser.add_argument('-O', '--outDir', dest='outDir', default="C:\DataBatch_ETF_NewProject\Output", help='Output Directory')
    parser.add_argument('-S', '--start', dest='start', default='', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-E', '--end', dest='end', default='', help='End date in dd/mm/yyyy format')
    parser.add_argument('-F', '--file_name', dest='file_name', default='Country_fundamentals_update.csv', help='Output filename')
    args = parser.parse_args()
    return args


def getIndicesFromFile(args):
    filename = os.path.join(args.inputDir,'Valuation_data.csv')
    index_list = list()
    with open(filename) as index_file:
        csv_index = csv.DictReader(index_file)
        for line in csv_index:
            index_list.append(line['Substitute Index'] + " Index")
    return index_list


def getFielddata(index, index_data, field):
    dates = sorted(index_data)
    data_tupple = list()
    for monthenddate in dates:
        monthendata = index_data[monthenddate]
        data_tupple.append(
            (monthenddate.strftime("%Y-%m-%d"), field.upper(), monthendata[field], index.replace(' Index', '')))
    return data_tupple


def writeTocsv(args, toprint):
    outputfile = os.path.join(args.outDir, args.file_name)
    with open(outputfile, 'w', newline='') as op_fh:
        writer = csv.writer(op_fh)
        header = ('date','variable','value','x')
        writer.writerow(header)
        for line in toprint:
            writer.writerow(line)


def clearFile(args):
    outputfile = os.path.join(args.outDir, args.file_name)
    if os.path.exists(outputfile):
        os.remove(outputfile)


def main():
    args = processOptions()
    last_month = datetime.datetime.today() - relativedelta(months=1)
    first_day_of_last_month = last_month.replace(day=1)
    start_to_use = first_day_of_last_month - timedelta(days=1)
    args.start = start_to_use.strftime('%d/%m/%Y') if args.start == '' else args.start
    args.end = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        '%d/%m/%Y') if args.end == '' else args.end
    startDate = datetime.datetime.strptime(args.start, '%d/%m/%Y')
    endDate = datetime.datetime.strptime(args.end, '%d/%m/%Y')
    clearFile(args)

    tickers = getIndicesFromFile(args)
    if len(tickers) == 0:
        print('No tickers specified. Exiting...')
        sys.exit(1)

    for field in FIELDS_LIST:
        bulkdata = bbgClient.remoteBbgHistoricalQuery('Historical Data', tickers, [field], startDate, endDate,
                                                      period='MONTHLY', periodAdjust='CALENDAR')
        for index in list(bulkdata.keys()):
            index_data = bulkdata[index]
            if not 'toprint' in locals():
                toprint = getFielddata(index, index_data, field)
            else:
                toprint.extend(getFielddata(index, index_data, field))
    toprint = sorted(toprint, key = lambda x:x[3])
    writeTocsv(args, toprint)


if __name__ == "__main__":
    main()

