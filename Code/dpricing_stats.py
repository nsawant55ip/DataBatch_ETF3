
import os, datetime
import pandas as pd
import argparse
import DataBatchUtils as DBU

BASEpath = os.path.join('C:\\', 'DataBatch_ETF_NewProject')
OUTPUTpath = os.path.join(BASEpath, 'Output')
changes_file = os.path.join(OUTPUTpath, 'dpricing_changes.csv')
stat_file = os.path.join(OUTPUTpath, 'dpricing_stats.csv')


def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-R', '--recipient', dest='recipient', default='batch_monitor@55-ip.com', help='Supply this argument to change the default recipient', )
    parser.add_argument('-H', '--header', dest='header', default='CLOSE', help='Supply this argument to change the default header')
    args = parser.parse_args()
    return args


def get_dpricing_stats(header):
    dprices = pd.read_csv(changes_file)
    #dprices = pd.read_csv("C:\DataBatch_ETF_NewProject\dpricing\dpricing_changes.csv")
    dprices['Date'] = pd.to_datetime(dprices['Date'])
    dprices.set_index(['Date'], inplace=True)
    # sorted_dates = sorted(dprices.Date.unique())[-5:] # index used instead of date due to python2.6 issue
    # top_dates = dprices.loc[dprices.Date.isin(sorted_dates)]
    # df1 = pd.DataFrame(top_dates.groupby(['Date'])['CLOSE'].max().reset_index(name='max'))
    sorted_dates = sorted(dprices.index.unique())[-5:]
    top_dates = dprices.loc[dprices.index.isin(sorted_dates)]
    df1 = pd.DataFrame(top_dates.groupby(top_dates.index)[header].max().reset_index(name='MAX'))
    df2 = pd.DataFrame(top_dates.groupby(top_dates.index)[header].min().reset_index(name='MIN'))
    df3 = pd.DataFrame(top_dates.groupby(top_dates.index)[header].mean().reset_index(name='MEAN'))
    df4 = pd.DataFrame(top_dates.groupby(top_dates.index)[header].std().reset_index(name='STD'))
    df5 = pd.DataFrame(top_dates.groupby(top_dates.index)[header].count().reset_index(name='COUNT'))
    df6 = df1.merge(df2, on='Date').merge(df3, on='Date').merge(df4, on='Date').merge(df5, on='Date')
    # dprices['Date'] = dprices['Date'].dt.strftime('%m/%d/%Y') # datetime to date python2.6 issue
    # df6.set_index(['Date'], inplace=True)
    df6.to_csv(stat_file, index=False, sep=',', encoding='utf-8')
    #df6.to_csv("C:\DataBatch_ETF_NewProject\output\dpricing_stats.csv", index=False, sep=',', encoding='utf-8')
    df6_table = df6.to_html(index=False)
    return df6_table


def get_html_start():
    html_start = """\
                <html>
                  <head></head>
                  <body>"""
    return html_start


def get_html_end():
    html_end = """\
                  </body>
                </html>
                """
    return html_end


def main():
    args = processOptions()
    recipient = [args.recipient]
    attachment = []
    # attachment = [os.path.join(stat_path,'dpricing_stats.csv')] # not needed
    try:
        header = args.header
        if header == 'close':
            header = 'CLOSE'
        dpricing_table = get_dpricing_stats(header)
        dpricing_table = dpricing_table.replace('<tr style="text-align: right;">', '<tr>')
        #dtstamp = datetime.datetime.fromtimestamp(os.path.getctime("C:\DataBatch_ETF_NewProject\dpricing\dpricing_changes.csv")).strftime('%Y-%m-%d %H:%M:%S')
        dtstamp = datetime.datetime.fromtimestamp(os.path.getctime(changes_file)).strftime('%Y-%m-%d %H:%M:%S')
        html_message = """\
                        <p>Please find the dpricing stats for column %s <br/></p>
                        <p>Source: %s <br/></p>
                        <p>Timestamp: %s <br/></p>
                    """ % (args.header, changes_file, dtstamp)
        content = get_html_start() + html_message + dpricing_table + get_html_end()

        DBU.SendRobustEmail(recipient, attachment, content, os.environ.get('COMPUTERNAME', 'UNKNOWN') + ' ETF Dpricing Stats')
    except Exception as e:
        error_message = "The ETF dpricing stat execution stopped with following error: "+str(e)
        DBU.SendRobustEmail(recipient, attachment, error_message, os.environ.get('COMPUTERNAME', 'UNKNOWN') + ' ETF Dpricing Stats Error!')


if __name__ == "__main__":
    main()
