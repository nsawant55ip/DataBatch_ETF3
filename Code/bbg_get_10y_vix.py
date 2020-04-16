#import bbgPxLast
import csv
import os, os.path
import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import argparse
import sys
import bbgClient
import ServerDataBatch as SDB

def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-B', '--basedir', dest='basedir', default=SDB.BASEpath, help='Base Directory')
    parser.add_argument('-O', '--outdir', dest='outdir', default="Output", help='Output Directory')
    #parser.add_argument('-S', '--start',   dest='start', default = '12/29/1989', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-S', '--start',   dest='start', default = '', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-E', '--end',     dest='end',   default = '', help='End date in dd/mm/yyyy format')
    args = parser.parse_args()
    #if args.end == '':
    #    args.end = datetime.datetime(datetime.datetime.now().year,12,31).strftime('%m/%d/%Y')
    #else:
    #    args.end = args.end
    return args

def _getEtfValue(data,ticker, date, field):
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

def saveOutputFile(data, outDir, fileName=None):
    if fileName is None or fileName == '':
        fileName = 'bbg_%s.csv' % datetime.datetime.now().strftime('%d%m%y_%H%M')
    try:
        outFile = open(os.path.join(outDir,fileName), "w", newline='')
        writer  = csv.writer(outFile)
        #Write the header of the csv
        writer.writerow(['date','10_y_prices','tyvix','euvix','jyvix','bpvix','gvzvix','ovxvix'])
        for date in sorted(data['TY1 Comdty']):  # Use dates for Sensex since its the oldest! This could change in the distant future...
            ty1    = _getEtfValue(data,'TY1 Comdty', date, 'PX_LAST')
            tyvix    = _getEtfValue(data,'TYVIX Index', date, 'PX_LAST')
            euvix    = _getEtfValue(data,'EUVIX Index', date, 'PX_LAST')
            jyvix    = _getEtfValue(data,'JYVIX Index', date, 'PX_LAST')
            bpvix    = _getEtfValue(data,'BPVIX Index', date, 'PX_LAST')
            gvzvix    = _getEtfValue(data,'GVZ Index', date, 'PX_LAST')
            ovxvix    = _getEtfValue(data,'OVX Index', date, 'PX_LAST')

            # Now to write the row to the file.  
            writer.writerow([date.strftime('%m/%d/%Y'), ty1, tyvix,euvix, jyvix, bpvix, gvzvix, ovxvix])

        outFile.close()
        
        #print('INFO: Finished writing file %s' % os.path.join(outDir, fileName))
    except Exception as e:
        print('Error : %s' % e)

def main():
    args = processOptions()
    daily_start_date = (datetime.datetime.now() - datetime.timedelta(days=30))
    args.start = daily_start_date.strftime('%d/%m/%Y') if args.start == '' else args.start
    args.end = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d/%m/%Y') if args.end == '' else args.end
    startDate = datetime.datetime.strptime(args.start, '%d/%m/%Y')
    endDate   = datetime.datetime.strptime(args.end, '%d/%m/%Y')
    tickers   = ['TY1 Comdty','TYVIX Index','EUVIX Index','JYVIX Index','BPVIX Index','GVZ Index','OVX Index']
    if len(tickers) == 0:
        print('No tickers specified. Exiting...')
        sys.exit(1)
    daily  = bbgClient.remoteBbgLatestPriceQuery('Etf monthly query',tickers, startDate,endDate, period='DAILY', ret=True,  periodAdjust='CALENDAR')
    output_path = os.path.join(args.basedir, args.outdir)
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    saveOutputFile(daily, output_path, fileName='10_Year_Treasury_and_VIX_update.csv')

    #print('INFO: Finished')

if __name__ == "__main__":
    main()
