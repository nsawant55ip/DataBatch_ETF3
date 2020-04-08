import csv
import os, os.path
import shutil
import datetime
import argparse
import bbgClient
from datetime import date, timedelta

def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-O','--outDir',   dest='outDir', default="C:\DataBatch_ETF_NewProject\Output", help='Output Directory')
    parser.add_argument('-S', '--start',   dest='start', default = '', help='Start date in mm/dd/yyyy format')
    parser.add_argument('-E', '--end',     dest='end',   default = '', help='End date in mm/dd/yyyy format')
    parser.add_argument('-C', '--codes',   dest='codes', default = "C:\DataBatch_ETF_NewProject\Inputs\world_bank_tickers.csv", help='The tickers file.')
    parser.add_argument('-D', '--staticdir', dest='staticdir', default="C:\DataBatch_ETF_NewProject\static",
                      help='static directory to store delta mode output')
    parser.add_argument('--run', default=False, action='store_true', help='Set this true to run now')
    args = parser.parse_args()
    args.start = datetime.datetime(datetime.datetime.now().year - 3, 1, 1).strftime(
        '%m/%d/%Y') if args.start == '' else args.start
    args.end = datetime.datetime(datetime.datetime.now().year - 1, 12, 31).strftime(
        '%m/%d/%Y') if args.end == '' else args.end
    return args


def extractTickers(args):
    try:
        codes = {}
        #codeFile = file(args.codes, 'rb')
        codeFile = open(args.codes, 'r')
        reader = csv.DictReader(codeFile)
        for row in reader:
            codes[row['Ticker']] = row
        codeFile.close()
        #print('INFO: Extracted Prowess Codes from %s' % args.codes)
        return codes
    except Exception as e:
        print('Error : %s' % e)


def processQueryLatestPrice(tickers, start, end, period='DAILY', adjSplit=False, ret=False, periodAdjust='ACTUAL'):
    return bbgClient.remoteBbgLatestPriceQuery('Daily Pricing download',tickers, start, end, period=period, adjSplit=adjSplit, ret=ret, periodAdjust=periodAdjust)


def main():
    args = processOptions()
    static_output_file = os.path.join(args.staticdir, 'world_bank_data_update.csv')
    output_file = os.path.join(args.outDir, 'world_bank_data_update.csv')
    # create a static directory if not already present
    if not os.path.exists(args.staticdir):
        os.makedirs(args.staticdir)
    if args.run:
        execute = True
    else:
        today = date.today()
        last_day_of_year = date.today().replace(month=12, day=31)
        days_to_run = [last_day_of_year-timedelta(days=1), last_day_of_year-timedelta(days=2),
                       last_day_of_year-timedelta(days=3), last_day_of_year-timedelta(days=4),
                       last_day_of_year-timedelta(days=5)]
        if today in days_to_run:
            execute = True
        else:
            execute = False
    if execute:
        startDate = datetime.datetime.strptime(args.start,'%m/%d/%Y')
        endDate   = datetime.datetime.strptime(args.end,'%m/%d/%Y')
        wb_tickers = extractTickers(args)
 
        # Download only the last three day's unadjusted close pricing
        # processQueryLatestPrice(wb_tickers.keys(), startDate, endDate, period='ANNUAL')
        # bbgClient.remoteBbgHistoricalQuery('Historical Data', tickers, ['PX_LAST'], startDate, endDate, period='ANNUAL', periodAdjust='CALENDAR')
        raw_data = processQueryLatestPrice(list(wb_tickers.keys()), startDate, endDate, period='YEARLY')
        #outFile = file(os.path.join(args.outDir, 'world_bank_data.csv'), 'wb')
        outFile = open(os.path.join(args.outDir, 'world_bank_data.csv'), 'w')
        #print("date,ticker,value,country,desc", file=outFile)
        for tick, d in raw_data.items():
            for currdate, currdata in d.items():
                try:
                    value = round(currdata['PX_LAST'], 4)
                except:
                    value = currdata['PX_LAST']
                country = wb_tickers[tick]['Country']
                desc = wb_tickers[tick]['Desc']
                print("%s,%s,%.4f,%s,%s" % (currdate.strftime("%m/%d/%Y"), tick, value, country, desc), file=outFile)

        outFile.close()
        print('INFO: Finished')
        # Also write this file into static folder to copy it from there during the delta mode.
        # And remove the existing file before copying
        if os.path.exists(static_output_file):
            os.remove(static_output_file)
        shutil.copy2(output_file, static_output_file)
    else:
        if os.path.exists(static_output_file):
            shutil.copy2(static_output_file, output_file)
        else:
            raise IOError("No file found in static folder")


if __name__ == "__main__":
    main()
    
