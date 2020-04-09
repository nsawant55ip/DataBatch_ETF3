#import bbgPxLast
import csv
import os, os.path
import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import argparse
import sys
import bbgClient

ticker_map = {  "FXE" : "EURUSD",
                "FXA" : "AUDUSD",
                "FXB" : "GBPUSD",
                "FXC" : "CADUSD",
                "FXF" : "CHFUSD",
                "FXS" : "SEKUSD",
                "FXY" : "JPYUSD",
                "INR" : "INRUSD",
                "BZF" : "BRLUSD"
              }
              
def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-O','--outDir',   dest='outDir', default="C:\DataBatch_ETF_NewProject\Output", help='Output Directory')
    # parser.add_argument('-S', '--start',   dest='start', default = '01/01/1997', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-S', '--start',   dest='start', default = '', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-E', '--end',     dest='end',   default = '', help='End date in dd/mm/yyyy format')
    args = parser.parse_args()
    if args.end == '':
        args.end = datetime.datetime(datetime.datetime.now().year,12,31).strftime('%m/%d/%Y')
    else:
        args.end = args.end
    return args

def _getEtfValue(data, ticker, date, field):
    value = ''
    if ticker in data:
        if date in data[ticker]:
            if data[ticker][date]['return'] != '':  # basically do not return anything if return is not defined.
                value = data[ticker][date][field]
        else:
            prevDate = date-datetime.timedelta(days=1)
            if (prevDate).month == date.month:
                value = _getEtfValue(data,ticker,prevDate,field)

    return value

def getkeyfromvalue(value):
    for k, v in ticker_map.items():
        if v == value.replace('CR Curncy',''):
            return k
            
def saveOutputFile(data, outDir, fileName=None):
    if fileName is None or fileName == '':
        fileName = 'bbg_%s.csv' % datetime.datetime.now().strftime('%d%m%y_%H%M')
    try:
        outFile = open(os.path.join(outDir,fileName), "w", newline='')
        writer  = csv.writer(outFile)
        writer.writerow(['Dates','Monthnumber','tick','tick_idx','return_idx','close_idx'])
        for i, tick_idx in enumerate(data.keys()):
            for date in sorted(data[tick_idx]):  
                tick          = getkeyfromvalue(tick_idx)
                return_idx    = _getEtfValue(data, tick_idx, date, 'return')
                close_idx     = _getEtfValue(data, tick_idx, date, 'PX_LAST')

                monthNum = date.month + (date.year - 1980)*12
                writer.writerow([date.strftime('%m/%d/%Y'), monthNum, tick, tick_idx.replace('CR Curncy',''), return_idx, close_idx ])
        outFile.close()
        #print('INFO: Finished writing file %s' % os.path.join(outDir, fileName))
    except Exception as e:
        print('Error : %s' % e)

def main():
    args = processOptions()
    last_month = datetime.datetime.today() - relativedelta(months=2)
    first_day_of_last_month = last_month.replace(day=1)
    start_to_use = first_day_of_last_month
    args.start = start_to_use.strftime('%m/%d/%Y') if args.start == '' else args.start
    # args.end = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d/%m/%Y') if args.end == '' else args.end
    startDate = datetime.datetime.strptime(args.start,'%m/%d/%Y')
    endDate   = datetime.datetime.strptime(args.end,'%m/%d/%Y')
    tickers   =  [curr + "CR Curncy" for curr in list(ticker_map.values())]
    if len(tickers) == 0:
        print('No tickers specified. Exiting...')
        sys.exit(1)

    monthly  = bbgClient.remoteBbgLatestPriceQuery('Etf monthly query',tickers, startDate,endDate, period='DAILY', adjSplit=True, ret=True, periodAdjust='CALENDAR')
    saveOutputFile(monthly, args.outDir, fileName='fx_etf_carry_daily_update.csv')

    #print('INFO: Finished')

if __name__ == "__main__":
    main()
