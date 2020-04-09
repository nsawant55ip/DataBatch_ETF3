import os, sys
import datetime
from dateutil.relativedelta import relativedelta
import csv
import argparse
import bbgClient
import shutil
from datetime import date, timedelta


def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('--outDir', dest='outDir', default="C:\DataBatch_ETF_NewProject\Output", help='Output Directory')
    parser.add_argument('--outFile', dest='outFile', default="projected_dividends.csv", help='Output Filename')
    parser.add_argument('--inFile', dest='inFile', default="historical_dividends.csv", help='Output Filename')
    parser.add_argument('-D', '--staticdir', dest='staticdir', default="C:\DataBatch_ETF_NewProject\static",
                      help='static directory to store delta mode output')
    parser.add_argument('--run', default=False, action='store_true', help='Set this true to run now')
    args = parser.parse_args()
    return args


def getValidTickerMap(args):
    """Return the ticker and mqaid map for valid ticker values"""
    dividend_file = os.path.join(args.outDir, args.inFile)
    with open(dividend_file) as divfh:
        csvdiv = csv.DictReader(divfh)
        tick_mqaid_map = dict()
        for lines in csvdiv:
            tick = lines["tick"]
            if tick == "Null":
                continue
            else:
                tick_mqaid_map[tick] = lines["mqaid"]
    return tick_mqaid_map


def parseBBgdata(data, bbg_tickers, tick_mqaid_map):
    """Iterate through the huge dict of dicts that is returned by Bloomnerg
    and get data in a line for each record. Combine all lines to get final data"""
    count_future_div_available = 0
    count_future_div_missing = 0
    alltickerlist = list()
    for ticker in bbg_tickers:
        try:
            ticker_data_dict = data[ticker]["BDVD_ALL_PROJECTIONS"]
            tick = ticker.replace(' US Equity','')
            mqaid = tick_mqaid_map[tick]
            for i, divdata in ticker_data_dict.items():
                dividend = divdata['Amount Per Share']
                div_ex_date = divdata['Ex-Date'].strftime("%m/%d/%Y")
                dataitems = [div_ex_date, mqaid, tick, dividend, None]
                alltickerlist.append(dataitems)
            count_future_div_available += 1
        except KeyError:
            count_future_div_missing += 1
    print("count_future_div_available : %s" %count_future_div_available)
    print("count_future_div_missing   : %s" %count_future_div_missing)
    return alltickerlist

def writeToFile(futuredatalist, args):
    outputfile = os.path.join(args.outDir, args.outFile)
    with open(outputfile,'w', newline='') as opfh:
        csvwrite = csv.writer(opfh)
        csvwrite.writerow(["divExDate","mqaid","tick","div_amount","monthclose"])
        for line in futuredatalist:
            csvwrite.writerow(line)


def main():
    """Controled flow of the program here."""
    args = processOptions()
    static_output_file = os.path.join(args.staticdir, args.outFile)
    output_path_file = os.path.join(args.outDir, args.outFile)
    # create a static directory if not already present
    if not os.path.exists(args.staticdir):
        os.makedirs(args.staticdir)
    if args.run:
        execute = True
    else:
        today = date.today()
        last_day_of_year = date.today().replace(month=12, day=31)
        days_to_run = [last_day_of_year - timedelta(days=1),
                       last_day_of_year - timedelta(days=2),
                       last_day_of_year - timedelta(days=3),
                       last_day_of_year - timedelta(days=4),
                       last_day_of_year - timedelta(days=5)]
        if today in days_to_run:
            execute = True
        else:
            execute = False
    if execute:
        tick_mqaid_map = getValidTickerMap(args)
        bbg_tickers = [tick + " US Equity" for tick in tick_mqaid_map]

        data = bbgClient.remoteBbgReferenceData('Corporate Actions',
                                                bbg_tickers,
                                                ['BDVD_ALL_PROJECTIONS'])
        futuredatalist = parseBBgdata(data, bbg_tickers, tick_mqaid_map)
        writeToFile(futuredatalist, args)
        # Also write this file into static folder to copy it from there during the delta mode.
        # And remove the existing file before copying
        if os.path.exists(static_output_file):
            os.remove(static_output_file)
        shutil.copy2(output_path_file, static_output_file)
    else:
        if os.path.exists(static_output_file):
            shutil.copy2(static_output_file, output_path_file)
        else:
            raise IOError("No file found in static folder")


if __name__ == "__main__":
    main()
