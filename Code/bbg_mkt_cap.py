
import bbgClient
import pandas as pd
import argparse
import os
import datetime


def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-O', '--outDir', dest='outDir', default="C:\DataBatch_ETF_NewProject\Output", help='Output Directory')
    args = parser.parse_args()
    return args


def main():
    args = processOptions()
    name = 'Corporate Actions'
    tickers = ['SPX Index', 'MID Index', 'SML Index', 'RAY Index', 'RIY Index', 'RTY Index', 'RUJ Index', 'RUO Index', 'MIDG Index', 'MIDV Index']
    fields = ['CUR_MKT_CAP']
    data = bbgClient.remoteBbgReferenceData(name, tickers, fields)
    df = pd.DataFrame.from_dict(data, orient='index').transpose()
    df.rename(columns = {'MID Index': 'mid', 'SML Index' : 'small', 'SPX Index' : 'large', 'RAY Index' : 'r3000',
                         'RIY Index' : 'r1000', 'RTY Index' : 'r2000', 'RUJ Index' : 'r2000_growth', 'RUO Index' : 'r2000_value',
                         'MIDG Index' : 'mid_growth', 'MIDV Index' : 'mid_value'}, inplace=True)
    df['date'] = datetime.date.today()
    df = df[['date','large','mid','small','r1000', 'r3000', 'r2000', 'r2000_growth', 'r2000_value', 'mid_growth', 'mid_value']]
    output_file = os.path.join(args.outDir, 'bbg_mkt_cap.csv')
    df.to_csv(output_file, sep=',', encoding='utf-8', index=False)


if __name__ == "__main__":
    main()
