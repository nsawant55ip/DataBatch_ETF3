import csv
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import argparse
import bbgClient
import pandas as pd

MONTHLY_FILE_NAME = "C:/DataBatch_ETF_NewProject/Output/US_IDX_IMP_M_update.csv"
DAILY_FILE_NAME = "C:/DataBatch_ETF_NewProject/Output/dpricing_changes_idx.csv"


def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--start', dest='start', default='', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-E', '--end', dest='end', default='', help='End date in dd/mm/yyyy format')
    parser.add_argument('-I', '--input', dest='input', default="C:\DataBatch_ETF_NewProject\Inputs\US_IDX_IMP_M_STATIC.csv",
                        help='The Static Input Filename to use when not -rerunning')
    parser.add_argument('-C', '--codes', dest='codes', default="C:\DataBatch_ETF_NewProject\Output\idx_univ.csv",
                      help='The prowess codes file.')
    parser.add_argument('-R', '--re_run', dest='re_run', default='', help='Rerun and download all data')
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
            pwsCodes[row['mqaid']] = row['tick']
        codeFile.close()
        print('INFO: Extracted Universe tickers from %s' % univfilename)
        return pwsCodes
    except Exception as e:
        print('Error : %s' % e)


def make_price_table(price_data):
    try:
        price_data_table = list()
        for index, values in price_data.iteritems():
            if index in price_data:
                for currdate, d in values.iteritems():
                    if ('PX_LAST' in d):
                        row = [currdate.strftime('%m/%d/%Y'), index, d['PX_LAST']]
                        price_data_table.append(row)
        price_data_table = pd.DataFrame(price_data_table)
        price_data_table.columns = ['date', 'tick_idx', 'close_idx']
        return price_data_table
    except Exception as e:
        print('Error : %s' % e)


def writeOutputFileWhenReRun(file_name, blmIndexes, monthlyprices):
    try:
        outFile = open(file_name, "wb", 0)
        writer = csv.writer(outFile)
        header = ['date', 'mqaid', 'tick_idx', 'close_idx', 'levtype', 'levamount', 'fundBenchmark', 'index_name']
        writer.writerow(header)
        for mqaid in blmIndexes:
            if blmIndexes[mqaid]['index'] in monthlyprices:
                for currdate, d in monthlyprices[blmIndexes[mqaid]['index']].iteritems():
                    if ('PX_LAST' in d):
                        writer.writerow([currdate.strftime('%m/%d/%Y'),
                                         mqaid, blmIndexes[mqaid]['index'].replace(' INDEX', ''),
                                         d['PX_LAST'], blmIndexes[mqaid]['levtype'],
                                         blmIndexes[mqaid]['levamount'], blmIndexes[mqaid]['fundBenchmark'],
                                         blmIndexes[mqaid]['index_name'].replace(u"\u2122", ' ')])

        outFile.close()
    except Exception as e:
        print('Error : %s' % e)


def extractBlmTickers(pwsCodes):
    try:
        return dict([(d['blm'], mqaid) for mqaid, d in pwsCodes.items() if d['blm'] != ''])
    except Exception as e:
        print('Error : %s' % e)


def getPricesFromBbg(tickers, start, end, period='DAILY', adjSplit=False, ret=False, periodAdjust='ACTUAL'):
    return bbgClient.remoteBbgLatestPriceQuery('Pricing download', tickers, start, end, period=period,
                                               adjSplit=adjSplit, ret=ret, periodAdjust=periodAdjust)


def getUndlIndexTicker(tickers):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['ETF_UNDL_INDEX_TICKER'])


def isFundLevered(tickers):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_LEVERAGE'])


def leverageType(tickers):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_LEVERAGE_TYPE'])


def leverageAmt(tickers):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_LEVERAGE_AMOUNT'])


def fundBenchmarkThird(tickers):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_BENCHMARK_THIRD'])


def fundFullName(tickers):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['NAME'])


def main():
    args = processOptions()
    last_month = datetime.datetime.today() - relativedelta(months=3)
    first_day_of_last_month = last_month.replace(day=1)
    start_to_use = first_day_of_last_month
    args.start = start_to_use.strftime('%m/%d/%Y') if args.start == '' else args.start

    startDate = datetime.datetime.strptime(args.start, '%m/%d/%Y')
    endDate = datetime.datetime.strptime(args.end, '%m/%d/%Y')

    if args.re_run:
        blmTicks = getUniverse(args.codes)
        blmIndexesraw = getUndlIndexTicker(list(blmTicks.values()))
        blmIsLevered = isFundLevered(list(blmTicks.values()))
        blmLeverageType = leverageType(list(blmTicks.values()))
        blmLeverageAmt = leverageAmt(list(blmTicks.values()))
        blmfundBenchmark = fundBenchmarkThird(list(blmTicks.values()))

        blmIndexes = dict()
        for mqaid, bbgtick in blmTicks.items():
            levered = 'N'
            leverType = 1
            leverAmt = 1.000
            index = ''
            fundBenchmark = ''

            if bbgtick in blmIndexesraw:
                if 'ETF_UNDL_INDEX_TICKER' in blmIndexesraw[bbgtick]:
                    index = '%s INDEX' % blmIndexesraw[bbgtick]['ETF_UNDL_INDEX_TICKER']

                if bbgtick in blmIsLevered:
                    if 'FUND_LEVERAGE' in blmIsLevered[bbgtick]:
                        levered = blmIsLevered[bbgtick]['FUND_LEVERAGE']

                if levered.upper() in ['Y', 'YES']:
                    try:
                        leverType = -1 if blmLeverageType[bbgtick]['FUND_LEVERAGE_TYPE'] in ['Short', 'SHORT'] else 1
                    except:
                        leverType = 1

                    try:
                        amounttemp = blmLeverageAmt[bbgtick]['FUND_LEVERAGE_AMOUNT']
                        amounttemp = amounttemp.replace('%', '').replace(' ', '')
                        leverAmt = float(amounttemp) / 100
                    except:
                        leverAmt = 1.000

                if bbgtick in blmfundBenchmark:
                    if 'FUND_BENCHMARK_THIRD' in blmfundBenchmark[bbgtick]:
                        fundBenchmark = blmfundBenchmark[bbgtick]['FUND_BENCHMARK_THIRD']

                blmIndexes[mqaid] = {'tick': bbgtick, 'index': index, 'levered': levered.upper(),
                                     'levtype': leverType, 'levamount': leverAmt, 'fundBenchmark': fundBenchmark}

        indxForQuery = [blmIndexes[mqaid]['index'] for mqaid in blmIndexes]
        blmIndexName = fundFullName(indxForQuery)

        for mqaid in blmIndexes:
            if blmIndexes[mqaid]['index'] in blmIndexName:
                blmIndexes[mqaid]['index_name'] = blmIndexName[blmIndexes[mqaid]['index']]['NAME'].encode('ascii',
                                                                                                          'ignore')
            else:
                blmIndexes[mqaid]['index_name'] = ''

        monthlyprices = dict()
        if len(indxForQuery) > 0:
            monthlyprices = getPricesFromBbg(indxForQuery, startDate, endDate, period='MONTHLY',
                                             adjSplit=True, ret=False, periodAdjust='ACTUAL')
        writeOutputFileWhenReRun(MONTHLY_FILE_NAME, blmIndexes, monthlyprices)

        dailyprices = dict()
        if len(indxForQuery) > 0:
            dailyprices = getPricesFromBbg(indxForQuery, startDate, endDate, period='DAILY',
                                             adjSplit=True, ret=False, periodAdjust='ACTUAL')
        writeOutputFileWhenReRun(DAILY_FILE_NAME, blmIndexes, dailyprices)
    else:
        input_file = pd.read_csv(args.input)
        indxForQuery = input_file.tick_idx.unique().tolist()

        if len(indxForQuery) > 0:
            # Get Monthly Data
            monthlyprices = getPricesFromBbg(indxForQuery, startDate, endDate, period='MONTHLY',
                                         adjSplit=True, ret=False, periodAdjust='ACTUAL')
            monthlyprices = make_price_table(monthlyprices)
            monthlyprices = input_file.merge(monthlyprices, on='tick_idx')
            monthlyprices.to_csv(MONTHLY_FILE_NAME, index=False)

            # Get Daily Data. Only for last 30 days
            endDate = datetime.datetime.today() - timedelta(days=1)
            startDate = endDate - timedelta(days=30)
            dailyprices = getPricesFromBbg(indxForQuery, startDate, endDate, period='DAILY',
                                         adjSplit=True, ret=False, periodAdjust='ACTUAL')
            dailyprices = make_price_table(dailyprices)
            dailyprices = input_file.merge(dailyprices, on='tick_idx')
            dailyprices.to_csv(DAILY_FILE_NAME, index=False)

    print('INFO: Finished')


if __name__ == "__main__":
    main()
