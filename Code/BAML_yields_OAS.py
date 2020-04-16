import urllib.request, urllib.error, urllib.parse
import argparse
import os, csv
import ServerDataBatch as SDB

link_list = ["https://research.stlouisfed.org/fred2/data/BAMLC0A0CM.txt",
             "https://research.stlouisfed.org/fred2/data/BAMLH0A0HYM2.txt",
             "https://research.stlouisfed.org/fred2/data/BAMLHE00EHYIOAS.txt",
             "https://research.stlouisfed.org/fred2/data/BAMLEMHBHYCRPIOAS.txt"]

def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-B', '--basedir', dest='basedir', default=SDB.BASEpath, help='Base Directory')
    parser.add_argument('-O', '--outdir', dest='outdir', default="Output", help='Output Directory')
    parser.add_argument('-F', '--filename', dest='filename', default="BAML_yields_OAS.csv", help='Output filename')
    args = parser.parse_args()
    return args

def download(url):
    """Download data from the links provided using simple python libs
    and convert the data ito a key value pait 0f date:value"""
    response = urllib.request.urlopen(url)
    html = response.read()
    alllines = html.decode('utf8').split('\r\n')
    # alllines = html.split('\r\n')
    flag = 0
    date_dict = dict()
    for line in alllines:
        if "DATE" in line and "VALUE" in line:
            flag = 1
            continue
        elif flag == 1:
            try:
                tempvar = line.split()
                date_dict[tempvar[0]] = tempvar[1]
            except IndexError:
                print(line)
    return date_dict

def getValueforDate(date,dict):
    """Check if the dict has the date key which is pased as first parameter"""
    for k in list(dict.keys()):
        if k == date:
            return dict[k]
    return None
    
def simpleMergeData(all_dates, dict_list):
    """With the assumption that startdate and enddate are same,
    simply merging the two tables into one"""
    lineslist = list()
    for date in all_dates:
        lineValues = [date]
        for dict in dict_list:
            value = getValueforDate(date,dict)
            lineValues.append(value)
        lineslist.append(lineValues)
    return lineslist

def writeTocsv(args, table):
    """Simply write a csv file based on the data we have"""
    output_path = os.path.join(args.basedir, args.outdir)
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    outputfile = os.path.join(output_path, args.filename)
    with open(outputfile, 'w', newline='') as op_fh:
        writer = csv.writer(op_fh)
        header = ('date','c0a0','h0a0','eurohyoas','emhyoas')
        writer.writerow(header)
        for line in table:
            writer.writerow(line)

def main():
    """Download data from the fred website. very primitive code.
    Usable enough for the purpose"""
    args = processOptions()
    #Download data from the websites and make a dict
    dict_list = []
    for link in link_list:
        data = download(link)
        dict_list.append(data)
    all_dates = []
    for data in dict_list:
        dates = sorted(data.keys())
        all_dates = all_dates + dates
    all_dates = sorted(list(set(all_dates)))
    # Merge all dicts to one simple table
    table = simpleMergeData(all_dates, dict_list)
    writeTocsv(args, table)

if __name__ == "__main__":
    main()
    
