import csv
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import argparse
import bbgClient


def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--start', dest='start', default='', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-E', '--end', dest='end', default='', help='End date in dd/mm/yyyy format')
    parser.add_argument('-M', '--monthly', dest='monthly', default="C:/DataBatch_ETF_NewProject/Output/fx_update.csv",
                      help='The Output Filename')
    parser.add_argument('-D', '--daily', dest='daily', default="C:/DataBatch_ETF_NewProject/Output/fx_daily_update.csv",
                      help='The Output Filename')
    parser.add_argument('-C', '--codes', dest='codes', default="C:\DataBatch_ETF_NewProject\Inputs\ccy_univ.csv",
                      help='The prowess codes file.')
    args = parser.parse_args()
    if args.end == '':
        args.end = datetime.datetime(datetime.datetime.now().year,12,31).strftime('%m/%d/%Y')
    else:
        args.end = args.end
    return args


def getUniverse(univfilename):
    try:
        pwsCodes = {}
        #codeFile = file(univfilename, 'rb')
        codeFile = open(univfilename, 'r')
        reader = csv.DictReader(codeFile)
        for row in reader:
            pwsCodes[row['mqaid']] = '%s Curncy' % row['blm']
        codeFile.close()
        print('INFO: Extracted Universe tickers from %s' % univfilename)
        return pwsCodes
    except Exception as e:
        print('Error : %s' % e)


def extractBlmTickers(pwsCodes):
    try:
        return dict([(d['blm'], mqaid) for mqaid, d in pwsCodes.items() if d['blm'] != ''])
    except Exception as e:
        print('Error : %s' % e)


def getPricesFromBbg(tickers, start, end, period='DAILY', adjSplit=False, ret=False, periodAdjust='ACTUAL', exch='US'):
    return bbgClient.remoteBbgLatestPriceQuery('Pricing download', tickers, start, end, period=period,
                                               adjSplit=adjSplit, ret=ret, periodAdjust=periodAdjust)


def main():
    args = processOptions()
    last_month = datetime.datetime.today() - relativedelta(months=2)
    first_day_of_last_month = last_month.replace(day=1)
    start_to_use = first_day_of_last_month

    args.start = start_to_use.strftime('%m/%d/%Y') if args.start == '' else args.start
    # args.end = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
    #     '%d/%m/%Y') if args.end == '' else args.end
    startDate = datetime.datetime.strptime(args.start, '%m/%d/%Y')
    endDate = datetime.datetime.strptime(args.end, '%m/%d/%Y')
    blmTicks = getUniverse(args.codes)
    monthlyprices = getPricesFromBbg(list(blmTicks.values()), startDate, endDate, period='MONTHLY', adjSplit=True, ret=True,
                                     periodAdjust='ACTUAL')

    try:
        outFile = open(args.monthly, "w", newline='')
        writer = csv.writer(outFile)
        header = ['date', 'mqaid', 'blm', 'close', 'return']
        writer.writerow(header)
        for mqaid, blm in blmTicks.items():
            if blm in monthlyprices:
                for currdate, d in monthlyprices[blm].items():
                    if ('PX_LAST' in d) and ('return' in d):
                        writer.writerow([currdate.strftime('%m/%d/%Y'), mqaid, blm.replace(' Curncy', ''), d['PX_LAST'],
                                         d['return']])

        outFile.close()
        # print('INFO: Finished writing file %s' % args.monthly)
    except Exception as e:
        print('Error : %s' % e)

    #print('INFO: Finished monthly')
    daily_start_date = (datetime.datetime.now() - datetime.timedelta(days=30))
    dailyprices = getPricesFromBbg(list(blmTicks.values()), daily_start_date, endDate, period='DAILY', adjSplit=True,
                                   ret=True, periodAdjust='ACTUAL')
    try:
        outFile = open(args.daily, "w", newline='')
        writer = csv.writer(outFile)
        header = ['date', 'mqaid', 'blm', 'close', 'return']
        writer.writerow(header)
        for mqaid, blm in blmTicks.items():
            if blm in dailyprices:
                for currdate, d in dailyprices[blm].items():
                    if ('PX_LAST' in d) and ('return' in d):
                        writer.writerow([currdate.strftime('%m/%d/%Y'), mqaid, blm.replace(' Curncy', ''), d['PX_LAST'],
                                         d['return']])

        outFile.close()
        #print('INFO: Finished writing file %s' % args.daily)
    except Exception as e:
        print('Error : %s' % e)

    print('INFO: Finished daily')


if __name__ == "__main__":
    main()
