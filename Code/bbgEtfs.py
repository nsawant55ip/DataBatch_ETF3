import csv
import os, os.path
import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import argparse
import sys
import bbgClient


def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-O', '--outDir', dest='outDir', default="C:\C:\DataBatch_ETF_NewProject\Output", help='Output Directory')
    # parser.add_argument('-S', '--start',   dest='start', default = '01/01/1987', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-S', '--start', dest='start', default='', help='Start date in dd/mm/yyyy format')
    parser.add_argument('-E', '--end', dest='end', default='', help='End date in dd/mm/yyyy format')
    args = parser.parse_args()
    return args


def _getEtfValue(data, ticker, date, field):
    value = ''
    if ticker in data:
        if date in data[ticker]:
            if data[ticker][date]['return'] != '':  # basically do not return anything if return is not defined.
                value = data[ticker][date][field]
        else:
            prevDate = date - datetime.timedelta(days=1)
            if (prevDate).month == date.month:
                value = _getEtfValue(data, ticker, prevDate, field)

    return value


def saveOutputFile(data, outDir, fileName=None):
    if fileName is None or fileName == '':
        fileName = 'bbg_%s.csv' % datetime.datetime.now().strftime('%d%m%y_%H%M')
    try:
        outFile = open(os.path.join(outDir, fileName), "w", newline='')
        writer = csv.writer(outFile)
        # Write the header of the csv
        # writer.writerow(['Date','Monthnumber','crack','pcrack','pl_fut','ppl_fut','dxy','pdxy','splv_idx','psplv_idx','mtum_idx','pmtum_idx','skew','pskew',
        #                  'gc_fut','pgc_fut','silver','psilver','crude','pcrude','gas','pgas','cotton','pcotton','coffee','pcoffee','sugar','psugar','cocoa','pcocoa','corn','pcorn','soyabean','psoyabean','wheat','pwheat','wwheat','pwwheat','copper','pcopper',
        #                  'gc_fut3','pgc_fut3','silver3','psilver3','crude3','pcrude3','gas3','pgas3','cotton3','pcotton3','coffee3','pcoffee3','sugar3','psugar3','cocoa3','pcocoa3','corn3','pcorn3','soyabean3','psoyabean3','wheat3','pwheat3','wwheat3','pwwheat3','copper3','pcopper3',
        #                  'gc_fut6','pgc_fut6','silver6','psilver6','crude6','pcrude6','gas6','pgas6','cotton6','pcotton6','coffee6','pcoffee6','sugar6','psugar6','cocoa6','pcocoa6','corn6','pcorn6','soyabean6','psoyabean6','wheat6','pwheat6','wwheat6','pwwheat6','copper6','pcopper6',
        #                  'silver12','psilver12','crude12','pcrude12','gas12','pgas12','cotton12','pcotton12','coffee12','pcoffee12','corn12','pcorn12','soyabean12','psoyabean12','wheat12','pwheat12','wwheat12','pwwheat12','copper12','pcopper12',
        #                  'fv1','pfv1','ty1','pty1','gc1','pgc1','spx','pspx','msci_eafe','pmsci_eafe','msci_em','pmsci_em','msci_acwi_tr','pmsci_acwi_tr','msci_acwi',
        #                 'pmsci_acwi','barxaggprice','pbarxaggprice','barxaggemusdprice','pbarxaggemusdprice','barxaggasianpacificusdprice',
        #                 'pbarxaggasianpacificusdprice','barxaggeuropeusdprice','pbarxaggeuropeusdprice','spgsciprice','pspgsciprice',
        #                 'europrice','peuroprice','reitusaprice','preitusaprice','reitdevelopedxusaprice','preitdevelopedxusaprice','napmpmi','pnapmpmi',
        #                 'concconf','pconcconf','vxv','pvxv','us0003m','pus0003m',' us0006m','pus0006m','us0012m','pus0012m','ted','pted','lt12truu','plt12truu',
        #                 'alumin1','palumin1','crytr','pcrytr','crynetr','pcrynetr','ccitr','pccitr', 'emcb', 'pemcb','mxwo','pmxwo',
        #                 'goldspot', 'pgoldspot', 'slvspot', 'pslvspot','hfrxar','phfrxar','gb1m','pgb1m'])
        writer.writerow(
            ['Date', 'Monthnumber', 'crack', 'pcrack', 'pl_fut', 'ppl_fut', 'dxy', 'pdxy', 'splv_idx', 'psplv_idx',
             'mtum_idx', 'pmtum_idx', 'skew', 'pskew',
             'gc_fut', 'pgc_fut', 'silver', 'psilver', 'crude', 'pcrude', 'gas', 'pgas', 'cotton', 'pcotton', 'coffee',
             'pcoffee', 'sugar', 'psugar', 'cocoa',
             'pcocoa', 'corn', 'pcorn', 'soyabean', 'psoyabean', 'wheat', 'pwheat', 'wwheat', 'pwwheat', 'copper',
             'pcopper', 'gc_fut3', 'pgc_fut3', 'gc_fut6',
             'pgc_fut6', 'fv1', 'pfv1', 'ty1', 'pty1', 'gc1', 'pgc1', 'spx', 'pspx', 'msci_eafe', 'pmsci_eafe',
             'msci_em', 'pmsci_em', 'msci_acwi_tr',
             'pmsci_acwi_tr', 'msci_acwi', 'pmsci_acwi', 'barxaggprice', 'pbarxaggprice', 'barxaggemusdprice',
             'pbarxaggemusdprice', 'barxaggasianpacificusdprice',
             'pbarxaggasianpacificusdprice', 'barxaggeuropeusdprice', 'pbarxaggeuropeusdprice', 'spgsciprice',
             'pspgsciprice', 'europrice', 'peuroprice',
             'reitusaprice', 'preitusaprice', 'reitdevelopedxusaprice', 'preitdevelopedxusaprice', 'napmpmi',
             'pnapmpmi', 'concconf', 'pconcconf', 'vxv',
             'pvxv', 'us0003m', 'pus0003m', ' us0006m', 'pus0006m', 'us0012m', 'pus0012m', 'ted', 'pted', 'lt12truu',
             'plt12truu', 'alumin1', 'palumin1', 'crytr',
             'pcrytr', 'crynetr', 'pcrynetr', 'ccitr', 'pccitr', 'emcb', 'pemcb', 'mxwo', 'pmxwo', 'goldspot',
             'pgoldspot', 'slvspot', 'pslvspot', 'hfrxar', 'phfrxar', 'gb1m', 'pgb1m'])

        for date in sorted(data[
                               'DXY Curncy']):  # Use dates for Sensex since its the oldest! This could change in the distant future...
            crack = _getEtfValue(data, 'CRK321M1 Index', date, 'return')
            pl_fut = _getEtfValue(data, 'PL1 Comdty', date, 'return')
            dxy = _getEtfValue(data, 'DXY Curncy', date, 'return')
            splv = _getEtfValue(data, 'SP5LVIT Index', date, 'return')
            mtum = _getEtfValue(data, 'M2US000$ Index', date, 'return')
            skew = _getEtfValue(data, 'SKEW Index', date, 'return')

            gc_fut = _getEtfValue(data, 'GC1 COMB Comdty', date, 'return')
            silver = _getEtfValue(data, 'SAI1 Comdty', date, 'return')
            crude = _getEtfValue(data, 'CL1 Comdty', date, 'return')
            gas = _getEtfValue(data, 'NG1 Comdty', date, 'return')
            cotton = _getEtfValue(data, 'CT1 Comdty', date, 'return')
            coffee = _getEtfValue(data, 'KC1 Comdty', date, 'return')
            sugar = _getEtfValue(data, 'SB1 Comdty', date, 'return')
            cocoa = _getEtfValue(data, 'CC1 Comdty', date, 'return')
            corn = _getEtfValue(data, 'C 1 Comdty', date, 'return')
            soyabean = _getEtfValue(data, 'S 1 Comdty', date, 'return')
            wheat = _getEtfValue(data, 'W 1 Comdty', date, 'return')
            wwheat = _getEtfValue(data, 'KW1 Comdty', date, 'return')
            copper = _getEtfValue(data, 'HG1 Comdty', date, 'return')

            gc_fut3 = _getEtfValue(data, 'GC3 COMB Comdty', date, 'return')
            # silver3 =  _getEtfValue(data, 'SAI3 Comdty', date,'return')
            # crude3 =  _getEtfValue(data, 'CL3 Comdty', date,'return')
            # gas3 =  _getEtfValue(data, 'NG3 Comdty', date,'return')
            # cotton3 =  _getEtfValue(data, 'CT3 Comdty', date,'return')
            # coffee3 =  _getEtfValue(data, 'KC3 Comdty', date,'return')
            # sugar3 =  _getEtfValue(data, 'SB3 Comdty', date,'return')
            # cocoa3 =  _getEtfValue(data, 'CC3 Comdty', date,'return')
            # corn3 =  _getEtfValue(data, 'C 3 Comdty', date,'return')
            # soyabean3 =  _getEtfValue(data, 'S 3 Comdty', date,'return')
            # wheat3 =  _getEtfValue(data, 'W 3 Comdty', date,'return')
            # wwheat3 =  _getEtfValue(data, 'KW3 Comdty', date,'return')
            # copper3 =  _getEtfValue(data, 'HG3 Comdty', date,'return')

            gc_fut6 = _getEtfValue(data, 'GC6 COMB Comdty', date, 'return')
            # silver6 =  _getEtfValue(data, 'SAI6 Comdty', date,'return')
            # crude6 =  _getEtfValue(data, 'CL6 Comdty', date,'return')
            # gas6 =  _getEtfValue(data, 'NG6 Comdty', date,'return')
            # cotton6 =  _getEtfValue(data, 'CT6 Comdty', date,'return')
            # coffee6 =  _getEtfValue(data, 'KC6 Comdty', date,'return')
            # sugar6 =  _getEtfValue(data, 'SB6 Comdty', date,'return')
            # cocoa6 =  _getEtfValue(data, 'CC6 Comdty', date,'return')
            # corn6 =  _getEtfValue(data, 'C 6 Comdty', date,'return')
            # soyabean6 =  _getEtfValue(data, 'S 6 Comdty', date,'return')
            # wheat6 =  _getEtfValue(data, 'W 6 Comdty', date,'return')
            # wwheat6 =  _getEtfValue(data, 'KW6 Comdty', date,'return')
            # copper6 =  _getEtfValue(data, 'HG6 Comdty', date,'return')

            # silver12 =  _getEtfValue(data, 'SAI12 Comdty', date,'return')
            # crude12 =  _getEtfValue(data, 'CL12 Comdty', date,'return')
            # gas12 =  _getEtfValue(data, 'NG12 Comdty', date,'return')
            # cotton12 =  _getEtfValue(data, 'CT12 Comdty', date,'return')
            # coffee12 =  _getEtfValue(data, 'KC12 Comdty', date,'return')
            # corn12 =  _getEtfValue(data, 'C 12 Comdty', date,'return')
            # soyabean12 =  _getEtfValue(data, 'S 12 Comdty', date,'return')
            # wheat12 =  _getEtfValue(data, 'W 12 Comdty', date,'return')
            # wwheat12 =  _getEtfValue(data, 'KW12 Comdty', date,'return')
            # copper12 =  _getEtfValue(data, 'HG12 Comdty', date,'return')
            fv1 = _getEtfValue(data, 'FV1 Comdty', date, 'return')
            ty1 = _getEtfValue(data, 'TY1 Comdty', date, 'return')
            gc1 = _getEtfValue(data, 'GC1 Comdty', date, 'return')

            spx = _getEtfValue(data, 'spx index', date, 'return')
            msci_eafe = _getEtfValue(data, 'mxea index', date, 'return')
            msci_em = _getEtfValue(data, 'mxef index', date, 'return')
            msci_acwi_tr = _getEtfValue(data, 'ndueacwf index', date, 'return')
            msci_acwi = _getEtfValue(data, 'mxwd index', date, 'return')
            barxaggprice = _getEtfValue(data, 'lbustruu index', date, 'return')
            barxaggemusdprice = _getEtfValue(data, 'emustruu index', date, 'return')
            barxaggasianpacificusdprice = _getEtfValue(data, 'lapctruh index', date, 'return')
            barxaggeuropeusdprice = _getEtfValue(data, 'lp06treh index', date, 'return')
            spgsciprice = _getEtfValue(data, 'spgscitr index', date, 'return')
            europrice = _getEtfValue(data, 'eurusd curncy', date, 'return')
            reitusaprice = _getEtfValue(data, 'unus index', date, 'return')
            reitdevelopedxusaprice = _getEtfValue(data, 'regxu index', date, 'return')
            napmpmi = _getEtfValue(data, 'napmpmi index', date, 'return')
            concconf = _getEtfValue(data, 'CONCCONF Index', date, 'return')
            vxv = _getEtfValue(data, 'VXV Index', date, 'return')
            us0003m = _getEtfValue(data, 'US0003M Index', date, 'return')
            us0006m = _getEtfValue(data, 'US0006M Index', date, 'return')
            us0012m = _getEtfValue(data, 'US0012M Index', date, 'return')
            ted = _getEtfValue(data, 'BASPTDSP Index', date, 'return')
            lt12truu = _getEtfValue(data, 'LT12TRUU Index', date, 'return')
            alumin1 = _getEtfValue(data, 'LA1 Comdty', date, 'return')
            crytr = _getEtfValue(data, 'CRYTR Index', date, 'return')
            crynetr = _getEtfValue(data, 'CRYNETR Index', date, 'return')
            ccitr = _getEtfValue(data, 'CCITR Index', date, 'return')
            emcb = _getEtfValue(data, 'EMCB Index', date, 'return')
            mxwo = _getEtfValue(data, 'MXWO Index', date, 'return')
            goldspot = _getEtfValue(data, 'XAU Curncy', date, 'return')
            slvspot = _getEtfValue(data, 'XAG Curncy', date, 'return')
            hfrxar = _getEtfValue(data, 'HFRXAR Index', date, 'return')
            gb1m = _getEtfValue(data, 'GB1M Index', date, 'return')

            # if all returns blank then skip writing row!
            if crack == '' and pl_fut == '' and dxy == '' and splv == '' and mtum == '' and skew == '':
                continue

            pcrack = _getEtfValue(data, 'CRK321M1 Index', date, 'PX_LAST')
            ppl_fut = _getEtfValue(data, 'PL1 Comdty', date, 'PX_LAST')
            pdxy = _getEtfValue(data, 'DXY Curncy', date, 'PX_LAST')
            psplv = _getEtfValue(data, 'SP5LVIT Index', date, 'PX_LAST')
            pmtum = _getEtfValue(data, 'M2US000$ Index', date, 'PX_LAST')
            pskew = _getEtfValue(data, 'SKEW Index', date, 'PX_LAST')

            pgc_fut = _getEtfValue(data, 'GC1 COMB Comdty', date, 'PX_LAST')
            psilver = _getEtfValue(data, 'SAI1 Comdty', date, 'PX_LAST')
            pcrude = _getEtfValue(data, 'CL1 Comdty', date, 'PX_LAST')
            pgas = _getEtfValue(data, 'NG1 Comdty', date, 'PX_LAST')
            pcotton = _getEtfValue(data, 'CT1 Comdty', date, 'PX_LAST')
            pcoffee = _getEtfValue(data, 'KC1 Comdty', date, 'PX_LAST')
            psugar = _getEtfValue(data, 'SB1 Comdty', date, 'PX_LAST')
            pcocoa = _getEtfValue(data, 'CC1 Comdty', date, 'PX_LAST')
            pcorn = _getEtfValue(data, 'C 1 Comdty', date, 'PX_LAST')
            psoyabean = _getEtfValue(data, 'S 1 Comdty', date, 'PX_LAST')
            pwheat = _getEtfValue(data, 'W 1 Comdty', date, 'PX_LAST')
            pwwheat = _getEtfValue(data, 'KW1 Comdty', date, 'PX_LAST')
            pcopper = _getEtfValue(data, 'HG1 Comdty', date, 'PX_LAST')

            pgc_fut3 = _getEtfValue(data, 'GC3 COMB Comdty', date, 'PX_LAST')
            # psilver3 =  _getEtfValue(data, 'SAI3 Comdty', date,'PX_LAST')
            # pcrude3 =  _getEtfValue(data, 'CL3 Comdty', date,'PX_LAST')
            # pgas3 =  _getEtfValue(data, 'NG3 Comdty', date,'PX_LAST')
            # pcotton3 =  _getEtfValue(data, 'CT3 Comdty', date,'PX_LAST')
            # pcoffee3 =  _getEtfValue(data, 'KC3 Comdty', date,'PX_LAST')
            # psugar3 =  _getEtfValue(data, 'SB3 Comdty', date,'PX_LAST')
            # pcocoa3 =  _getEtfValue(data, 'CC3 Comdty', date,'PX_LAST')
            # pcorn3 =  _getEtfValue(data, 'C 3 Comdty', date,'PX_LAST')
            # psoyabean3 =  _getEtfValue(data, 'S 3 Comdty', date,'PX_LAST')
            # pwheat3 =  _getEtfValue(data, 'W 3 Comdty', date,'PX_LAST')
            # pwwheat3 =  _getEtfValue(data, 'KW3 Comdty', date,'PX_LAST')
            # pcopper3 =  _getEtfValue(data, 'HG3 Comdty', date,'PX_LAST')

            pgc_fut6 = _getEtfValue(data, 'GC6 COMB Comdty', date, 'PX_LAST')
            # psilver6 =  _getEtfValue(data, 'SAI6 Comdty', date,'PX_LAST')
            # pcrude6 =  _getEtfValue(data, 'CL6 Comdty', date,'PX_LAST')
            # pgas6 =  _getEtfValue(data, 'NG6 Comdty', date,'PX_LAST')
            # pcotton6 =  _getEtfValue(data, 'CT6 Comdty', date,'PX_LAST')
            # pcoffee6 =  _getEtfValue(data, 'KC6 Comdty', date,'PX_LAST')
            # psugar6 =  _getEtfValue(data, 'SB6 Comdty', date,'PX_LAST')
            # pcocoa6 =  _getEtfValue(data, 'CC6 Comdty', date,'PX_LAST')
            # pcorn6 =  _getEtfValue(data, 'C 6 Comdty', date,'PX_LAST')
            # psoyabean6 =  _getEtfValue(data, 'S 6 Comdty', date,'PX_LAST')
            # pwheat6 =  _getEtfValue(data, 'W 6 Comdty', date,'PX_LAST')
            # pwwheat6 =  _getEtfValue(data, 'KW6 Comdty', date,'PX_LAST')
            # pcopper6 =  _getEtfValue(data, 'HG6 Comdty', date,'PX_LAST')

            # psilver12 =  _getEtfValue(data, 'SAI12 Comdty', date,'PX_LAST')
            # pcrude12 =  _getEtfValue(data, 'CL12 Comdty', date,'PX_LAST')
            # pgas12 =  _getEtfValue(data, 'NG12 Comdty', date,'PX_LAST')
            # pcotton12 =  _getEtfValue(data, 'CT12 Comdty', date,'PX_LAST')
            # pcoffee12 =  _getEtfValue(data, 'KC12 Comdty', date,'PX_LAST')
            # pcorn12 =  _getEtfValue(data, 'C 12 Comdty', date,'PX_LAST')
            # psoyabean12 =  _getEtfValue(data, 'S 12 Comdty', date,'PX_LAST')
            # pwheat12 =  _getEtfValue(data, 'W 12 Comdty', date,'PX_LAST')
            # pwwheat12 =  _getEtfValue(data, 'KW12 Comdty', date,'PX_LAST')
            # pcopper12 =  _getEtfValue(data, 'HG12 Comdty', date,'PX_LAST')
            pfv1 = _getEtfValue(data, 'FV1 Comdty', date, 'PX_LAST')
            pty1 = _getEtfValue(data, 'TY1 Comdty', date, 'PX_LAST')
            pgc1 = _getEtfValue(data, 'GC1 Comdty', date, 'PX_LAST')

            pspx = _getEtfValue(data, 'spx index', date, 'PX_LAST')
            pmsci_eafe = _getEtfValue(data, 'mxea index', date, 'PX_LAST')
            pmsci_em = _getEtfValue(data, 'mxef index', date, 'PX_LAST')
            pmsci_acwi_tr = _getEtfValue(data, 'ndueacwf index', date, 'PX_LAST')
            pmsci_acwi = _getEtfValue(data, 'mxwd index', date, 'PX_LAST')
            pbarxaggprice = _getEtfValue(data, 'lbustruu index', date, 'PX_LAST')
            pbarxaggemusdprice = _getEtfValue(data, 'emustruu index', date, 'PX_LAST')
            pbarxaggasianpacificusdprice = _getEtfValue(data, 'lapctruh index', date, 'PX_LAST')
            pbarxaggeuropeusdprice = _getEtfValue(data, 'lp06treh index', date, 'PX_LAST')
            pspgsciprice = _getEtfValue(data, 'spgscitr index', date, 'PX_LAST')
            peuroprice = _getEtfValue(data, 'eurusd curncy', date, 'PX_LAST')
            preitusaprice = _getEtfValue(data, 'unus index', date, 'PX_LAST')
            preitdevelopedxusaprice = _getEtfValue(data, 'regxu index', date, 'PX_LAST')
            pnapmpmi = _getEtfValue(data, 'napmpmi index', date, 'PX_LAST')
            pconcconf = _getEtfValue(data, 'CONCCONF Index', date, 'PX_LAST')
            pvxv = _getEtfValue(data, 'VXV Index', date, 'PX_LAST')
            pus0003m = _getEtfValue(data, 'US0003M Index', date, 'PX_LAST')
            pus0006m = _getEtfValue(data, 'US0006M Index', date, 'PX_LAST')
            pus0012m = _getEtfValue(data, 'US0012M Index', date, 'PX_LAST')
            pted = _getEtfValue(data, 'BASPTDSP Index', date, 'PX_LAST')
            plt12truu = _getEtfValue(data, 'LT12TRUU Index', date, 'PX_LAST')
            palumin1 = _getEtfValue(data, 'LA1 Comdty', date, 'PX_LAST')
            pcrytr = _getEtfValue(data, 'CRYTR Index', date, 'PX_LAST')
            pcrynetr = _getEtfValue(data, 'CRYNETR Index', date, 'PX_LAST')
            pccitr = _getEtfValue(data, 'CCITR Index', date, 'PX_LAST')
            pemcb = _getEtfValue(data, 'EMCB Index', date, 'PX_LAST')
            pmxwo = _getEtfValue(data, 'MXWO Index', date, 'PX_LAST')
            pgoldspot = _getEtfValue(data, 'XAU Curncy', date, 'PX_LAST')
            pslvspot = _getEtfValue(data, 'XAG Curncy', date, 'PX_LAST')
            phfrxar = _getEtfValue(data, 'HFRXAR Index', date, 'PX_LAST')
            pgb1m = _getEtfValue(data, 'GB1M Index', date, 'PX_LAST')

            monthNum = date.month + (date.year - 1980) * 12

            # Now to write the row to the file.
            # writer.writerow([date.strftime('%m/%d/%Y'), monthNum, crack,pcrack,pl_fut,ppl_fut,dxy,pdxy, splv, psplv, mtum, pmtum, skew, pskew,
            #                  gc_fut,pgc_fut,silver,psilver,crude,pcrude,gas,pgas,cotton,pcotton,coffee,pcoffee,sugar,psugar,cocoa,pcocoa,corn,pcorn,soyabean,psoyabean,wheat,pwheat,wwheat,pwwheat,copper,pcopper,
            #                  gc_fut3,pgc_fut3,silver3,psilver3,crude3,pcrude3,gas3,pgas3,cotton3,pcotton3,coffee3,pcoffee3,sugar3,psugar3,cocoa3,pcocoa3,corn3,pcorn3,soyabean3,psoyabean3,wheat3,pwheat3,wwheat3,pwwheat3,copper3,pcopper3,
            #                  gc_fut6,pgc_fut6,silver6,psilver6,crude6,pcrude6,gas6,pgas6,cotton6,pcotton6,coffee6,pcoffee6,sugar6,psugar6,cocoa6,pcocoa6,corn6,pcorn6,soyabean6,psoyabean6,wheat6,pwheat6,wwheat6,pwwheat6,copper6,pcopper6,
            #                  silver12,psilver12,crude12,pcrude12,gas12,pgas12,cotton12,pcotton12,coffee12,pcoffee12,corn12,pcorn12,soyabean12,psoyabean12,wheat12,pwheat12,wwheat12,pwwheat12,copper12,pcopper12,
            #                  fv1, pfv1,ty1,pty1,gc1,pgc1,spx,pspx,msci_eafe,pmsci_eafe,msci_em,pmsci_em,msci_acwi_tr,pmsci_acwi_tr,msci_acwi,pmsci_acwi,barxaggprice,pbarxaggprice,barxaggemusdprice,pbarxaggemusdprice,barxaggasianpacificusdprice,
            #                  pbarxaggasianpacificusdprice,barxaggeuropeusdprice,pbarxaggeuropeusdprice,spgsciprice,pspgsciprice,europrice,peuroprice,reitusaprice,preitusaprice,reitdevelopedxusaprice,preitdevelopedxusaprice,napmpmi,pnapmpmi,
            #                  concconf, pconcconf, vxv, pvxv, us0003m,pus0003m, us0006m,pus0006m,us0012m,pus0012m,ted,pted,lt12truu,plt12truu,alumin1,palumin1,crytr,pcrytr,crynetr,pcrynetr,ccitr,pccitr,
            #                  emcb, pemcb, mxwo, pmxwo, goldspot, pgoldspot, slvspot, pslvspot, hfrxar, phfrxar, gb1m, pgb1m
            #                  ])
            writer.writerow(
                [date.strftime('%m/%d/%Y'), monthNum, crack, pcrack, pl_fut, ppl_fut, dxy, pdxy, splv, psplv, mtum,
                 pmtum, skew, pskew,
                 gc_fut, pgc_fut, silver, psilver, crude, pcrude, gas, pgas, cotton, pcotton, coffee, pcoffee, sugar,
                 psugar, cocoa, pcocoa, corn, pcorn, soyabean, psoyabean, wheat, pwheat, wwheat, pwwheat, copper,
                 pcopper, gc_fut3, pgc_fut3, gc_fut6, pgc_fut6,
                 fv1, pfv1, ty1, pty1, gc1, pgc1, spx, pspx, msci_eafe, pmsci_eafe, msci_em, pmsci_em, msci_acwi_tr,
                 pmsci_acwi_tr, msci_acwi, pmsci_acwi, barxaggprice, pbarxaggprice, barxaggemusdprice,
                 pbarxaggemusdprice, barxaggasianpacificusdprice,
                 pbarxaggasianpacificusdprice, barxaggeuropeusdprice, pbarxaggeuropeusdprice, spgsciprice, pspgsciprice,
                 europrice, peuroprice, reitusaprice, preitusaprice, reitdevelopedxusaprice, preitdevelopedxusaprice,
                 napmpmi, pnapmpmi,
                 concconf, pconcconf, vxv, pvxv, us0003m, pus0003m, us0006m, pus0006m, us0012m, pus0012m, ted, pted,
                 lt12truu, plt12truu, alumin1, palumin1, crytr, pcrytr, crynetr, pcrynetr, ccitr, pccitr,
                 emcb, pemcb, mxwo, pmxwo, goldspot, pgoldspot, slvspot, pslvspot, hfrxar, phfrxar, gb1m, pgb1m
                 ])
        outFile.close()

        print('INFO: Finished writing file %s' % os.path.join(outDir, fileName))
    except Exception as e:
        print('Error : %s' % e)

def main():
    args = processOptions()
    last_month = datetime.datetime.today() - relativedelta(months=3)
    first_day_of_last_month = last_month.replace(day=1)
    start_to_use = first_day_of_last_month  #  - timedelta(days=1)
    args.start = start_to_use.strftime('%d/%m/%Y') if args.start == '' else args.start
    args.end = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        '%d/%m/%Y') if args.end == '' else args.end
    startDate = datetime.datetime.strptime(args.start, '%d/%m/%Y')
    endDate = datetime.datetime.strptime(args.end, '%d/%m/%Y')

    tickers = ['CRK321M1 Index', 'PL1 Comdty', 'DXY Curncy', 'SP5LVIT Index', 'M2US000$ Index', 'SKEW Index',
               'GC1 COMB Comdty', 'SAI1 Comdty', 'CL1 Comdty', 'NG1 Comdty', 'CT1 Comdty', 'KC1 Comdty', 'SB1 Comdty',
               'CC1 Comdty', 'C 1 Comdty', 'S 1 Comdty', 'W 1 Comdty', 'KW1 Comdty', 'HG1 Comdty',
               'GC3 COMB Comdty', 'GC6 COMB Comdty', 'FV1 Comdty',
               'TY1 Comdty', 'GC1 Comdty', 'spx index', 'mxea index', 'mxef index', 'ndueacwf index', 'mxwd index',
               'lbustruu index', 'emustruu index', 'lapctruh index', 'lp06treh index',
               'spgscitr index', 'eurusd curncy', 'unus index', 'regxu index', 'napmpmi index', 'CONCCONF Index',
               'VXV Index', 'US0003M Index', 'US0006M Index', 'US0012M Index', 'BASPTDSP Index', 'LT12TRUU Index',
               'LA1 Comdty', 'CRYTR Index', 'CRYNETR Index', 'CCITR Index', 'EMCB Index', 'MXWO Index', 'XAU Curncy',
               'XAG Curncy', 'HFRXAR Index', 'GB1M Index'
               ]
    if len(tickers) == 0:
        print('No tickers specified. Exiting...')
        sys.exit(1)

    daily_start_date = (datetime.datetime.now() - datetime.timedelta(days=60))
    daily = bbgClient.remoteBbgLatestPriceQuery('Etf daily query', tickers, daily_start_date, endDate, period='DAILY',
                                                adjSplit=True, ret=True)
    saveOutputFile(daily, args.outDir, fileName='bbg_etfs_daily_update.csv')

    monthlyenddate = datetime.datetime(endDate.year,12,31)
    monthly = bbgClient.remoteBbgLatestPriceQuery('Etf monthly query', tickers, startDate, monthlyenddate, period='MONTHLY',
                                                  adjSplit=True, ret=True)

    saveOutputFile(monthly, args.outDir, fileName='bbg_etfs_update.csv')

    print('INFO: Finished')


if __name__ == "__main__":
    main()
