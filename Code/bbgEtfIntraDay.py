import argparse
import datetime
import sys
import datetime
import os
import numpy
import csv
import bbgClient
from dateutil.relativedelta import relativedelta

def processOptions():
    """Process the command line args using 'argparse'"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--outputcsvdaily", default="C:/DataBatch_ETF_NewProject/Output/etfs_intraday_prc_update.csv", help="path to daily MOC csv file")
    args = parser.parse_args()
    return args
        
if __name__ == "__main__":
    args = processOptions()
    securities = {'SPY':'SPY US Equity',
                  '46428746': 'EFA US Equity',
                  '46428723': 'EEM US Equity',
                  'VIX':'VIX Index'}
    endDate   = datetime.datetime.now()-datetime.timedelta(days=1)
    startDate = endDate-datetime.timedelta(days=30)
    allSecDict = dict()
    file1 = open(args.outputcsvdaily, 'w')
    #print("mqaid,tick,datetime,close", file=file1)
    for mqaid, security in securities.items():
        currDate = startDate
        secDict = dict()
        #print(("Start %s" % security))
        while currDate < endDate:
            currDate = currDate + datetime.timedelta(days=1)
            if currDate.weekday() in [5,6]: 
                continue
            dataResponse = bbgClient.remoteBbgLatestPriceQuery('IntraDay', security, currDate.strftime('%Y-%m-%d'))
            #print(dataResponse)
            secDict.update(dataResponse)
            
        if len(secDict) > 0:
            for dates in sorted(secDict):
                print(("%s,%s,%s,%.2f" % (mqaid,security, dates.strftime("%m/%d/%Y:%H:%M"), secDict[dates])), file=file1)

    file1.close()


