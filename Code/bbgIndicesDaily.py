
import csv
import os, os.path
import csv
import shutil
import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import argparse
import sys
import bbgClient


def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--start',   dest='start',  default = '01/01/1997', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-E', '--end',     dest='end',    default = '', help='End date in dd/mm/yyyy format')
    parser.add_argument('-O', '--output',  dest='output', default = "C:\DataBatch_ETF_NewProject\Output\dpricing_changes_idx.csv", help='The Output Filename')
    parser.add_argument('-C', '--codes',   dest='codes',  default = "C:\DataBatch_ETF_NewProject\Output\idx_univ.csv", help='The prowess codes file.')
    args = parser.parse_args()
    if args.end == '':
        args.end = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%m/%d/%Y')
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
        #print('INFO: Extracted Universe tickers from %s' % univfilename)
        return pwsCodes
    except Exception as e:
        print('Error : %s' % e)
        
def extractBlmTickers(pwsCodes):
    try:
        return dict([(d['blm'], mqaid) for mqaid, d in pwsCodes.items() if d['blm'] != ''])        
    except Exception as e:
        print('Error : %s' % e)
   

def getPricesFromBbg(tickers, start, end, period='DAILY', adjSplit=False, ret=False, periodAdjust='ACTUAL', exch='US'):   
    return bbgClient.remoteBbgLatestPriceQuery('Pricing download',tickers, start, end, period=period, adjSplit=adjSplit, ret=ret, periodAdjust=periodAdjust)

def getUndlIndexTicker(tickers, exch='US'):   
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['ETF_UNDL_INDEX_TICKER'])

def isFundLevered(tickers, exch='US'):   
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_LEVERAGE'])

def leverageType(tickers, exch='US'):   
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_LEVERAGE_TYPE'])

def leverageAmt(tickers, exch='US'):   
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_LEVERAGE_AMOUNT'])
    
def fundBenchmarkThird(tickers, exch='US'):   
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_BENCHMARK_THIRD'])

def main():
    args = processOptions()
    endDate   = datetime.datetime.strptime(args.end,'%m/%d/%Y')
    startDate = endDate-datetime.timedelta(days=30)
    blmTicks = getUniverse(args.codes)
    #print("blmTicks",blmTicks)
    blmIndexesraw = getUndlIndexTicker(list(blmTicks.values()))
    blmIsLevered = isFundLevered(list(blmTicks.values()))
    blmLeverageType = leverageType(list(blmTicks.values()))
    blmLeverageAmt = leverageAmt(list(blmTicks.values()))
    blmfundBenchmark = fundBenchmarkThird(list(blmTicks.values()))
    blmIndexes = dict()
    for mqaid, bbgtick in blmTicks.items():
        levered      = 'N'
        leverType = 1
        leverAmt  = 1.000
        index = ''
        fundBenchmark = ''
        
        if bbgtick in blmIndexesraw:
            if 'ETF_UNDL_INDEX_TICKER' in blmIndexesraw[bbgtick]:
                index = '%s INDEX' % blmIndexesraw[bbgtick]['ETF_UNDL_INDEX_TICKER']
                
            if bbgtick in blmIsLevered:
                if 'FUND_LEVERAGE' in blmIsLevered[bbgtick]:
                    levered = blmIsLevered[bbgtick]['FUND_LEVERAGE']           
                
            if levered.upper() in ['Y','YES']:
                try:
                    leverType = -1 if blmLeverageType[bbgtick]['FUND_LEVERAGE_TYPE'] in ['Short', 'SHORT'] else 1
                except:
                    leverType = 1
    
                try:
                    amounttemp = blmLeverageAmt[bbgtick]['FUND_LEVERAGE_AMOUNT'] 
                    amounttemp = amounttemp.replace('%','').replace(' ','')
                    leverAmt = float(amounttemp)/100
                except:
                    leverAmt = 1.000
                    
            if bbgtick in blmfundBenchmark:
                if 'FUND_BENCHMARK_THIRD' in blmfundBenchmark[bbgtick]:
                    fundBenchmark = blmfundBenchmark[bbgtick]['FUND_BENCHMARK_THIRD']    
                    
            blmIndexes[mqaid] = {'tick': bbgtick, 'index':index,'levered':levered.upper(),
                                 'levtype':leverType,'levamount':leverAmt, 'fundBenchmark' : fundBenchmark}
    

    indxForQuery = [blmIndexes[mqaid]['index']for mqaid in blmIndexes]
    dailyprices = dict()
    if len(indxForQuery) > 0:
        dailyprices = getPricesFromBbg(indxForQuery, startDate, endDate, period='DAILY', 
                                         adjSplit=True, ret=False, periodAdjust='ACTUAL')
        
    try:
        outFile = open(args.output, "w", newline='')
        writer  = csv.writer(outFile)
        header = ['date','mqaid','tick_idx','close_idx','levtype','levamount', 'fundBenchmark']
        writer.writerow(header)
        for mqaid in blmIndexes:
            if blmIndexes[mqaid]['index'] in dailyprices:
                for currdate,d in dailyprices[blmIndexes[mqaid]['index']].items():
                    if ('PX_LAST' in d):
                        writer.writerow([currdate.strftime('%m/%d/%Y'), 
                                         mqaid, blmIndexes[mqaid]['index'].replace(' INDEX',''), 
                                         d['PX_LAST'], blmIndexes[mqaid]['levtype'],
                                         blmIndexes[mqaid]['levamount'], blmIndexes[mqaid]['fundBenchmark']])             

        outFile.close()
        #print('INFO: Finished writing file %s' % args.output)
    except Exception as e:
        print('Error : %s' % e)

    #print('INFO: Finished')
    
if __name__ == "__main__":
    main()
    
