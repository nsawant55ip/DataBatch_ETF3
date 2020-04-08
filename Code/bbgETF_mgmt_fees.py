import csv
import os
import shutil
import datetime
import argparse
import bbgClient
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd


def processOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-O', '--output', dest='output', default = "C:\DataBatch_ETF_NewProject\Output\ETF_mgmt_fees.csv",
                      help='The Output Filename')
    parser.add_argument('-C', '--codes', dest='codes', default = "C:\DataBatch_ETF_NewProject\Output\idx_univ_with_hist.csv",
                      help='The prowess codes file.')
    parser.add_argument('-D', '--staticdir', dest='staticdir', default="C:\DataBatch_ETF_NewProject\static",
                      help='static directory to store delta mode output')
    parser.add_argument('--run', default=False, action='store_true', help='Set this true to run now')
    args = parser.parse_args()
    return args


def getUniverse(univfilename):
    try:
        pwsCodes = {}
        # codeFile = file(univfilename, 'rb')
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


def expenseRatio(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_EXPENSE_RATIO'])


def bidAskSpread(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['AVERAGE_BID_ASK_SPREAD_%'])


def managementFee(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_MGR_STATED_FEE'])


def creationRedemptionFee(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['CREATION_FEE'])


def creationRedemptionSize(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_CREATION_UNIT_SIZE'])


def etfimpliedliquidity(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['ETF_IMPLIED_LIQUIDITY'])


def fundpctpremium(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_PCT_PREMIUM'])


def currencyhedgedindicator(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['CURRENCY_HEDGED_INDICATOR'])


def derivativesbased(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['DERIVATIVES_BASED'])


def securitieslending(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['SECURITIES_LENDING'])


def fundinceptdt(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_INCEPT_DT'])


def createredeemprocess(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['CREATE_REDEEM_PROCESS'])


def creationcutofftime(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['CREATION_CUTOFF_TIME'])


def settlementcycle(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['SETTLEMENT_CYCLE'])


def rebalancingfrequency(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['REBALANCING_FREQUENCY'])


def replicationstrategy(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['REPLICATION_STRATEGY'])


def fundportdt(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_PORT_DT'])


def totalnumberofholdingsinport(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['TOTAL_NUMBER_OF_HOLDINGS_IN_PORT'])


def navpricingmethodology(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['NAV_PRICING_METHODOLOGY'])


def fundavgdvdyld(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_AVG_DVD_YLD'])


def fundperatio(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_PE_RATIO'])


def fundpricingsource(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['FUND_PRICING_SOURCE'])


def sixmonthavgvolume(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['VOLUME_AVG_6M'])


def etfnamefrombloomberg(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['NAME'])


def etfdurationfrombloomberg(tickers, exch='US'):
    return bbgClient.remoteBbgReferenceData('Corporate Actions', tickers, ['YAS_MOD_DUR'])


def main():
    args = processOptions()
    static_output_file = os.path.join(args.staticdir, os.path.basename(args.output))
    # create a static directory if not already present
    if not os.path.exists(args.staticdir):
        os.makedirs(args.staticdir)
    if args.run:
        execute = True
    else:
        today = date.today()
        next_month = today + relativedelta(months=1)
        first_day_of_next_month = next_month.replace(day=1)
        last_day_of_month = first_day_of_next_month - timedelta(days=1)
        days_to_run = [last_day_of_month, last_day_of_month - timedelta(days=1), last_day_of_month - timedelta(days=2)]
        if today in days_to_run:
            execute = True
        else:
            execute = False
    if execute:
        args = processOptions()
        blmTicks = getUniverse(args.codes)
        blmmanagementFee = managementFee(list(blmTicks.values()))
        blmexpenseRatio = expenseRatio(list(blmTicks.values()))
        # blmbidAskSpread = bidAskSpread(blmTicks.values())
        # blmcreateRedeemFee = creationRedemptionFee(blmTicks.values())
        # blmcreateRedeemSize = creationRedemptionSize(blmTicks.values())
        # blmetfimpliedliquidity = etfimpliedliquidity(blmTicks.values())
        # blmfundpctpremium = fundpctpremium(blmTicks.values())
        # blmcurrencyhedgedindicator = currencyhedgedindicator(blmTicks.values())
        # blmderivativesbased = derivativesbased(blmTicks.values())
        # blmsecuritieslending = securitieslending(blmTicks.values())
        # blmfundinceptdt = fundinceptdt(blmTicks.values())
        # blmcreateredeemprocess = createredeemprocess(blmTicks.values())
        # blmcreationcutofftime = creationcutofftime(blmTicks.values())
        # blmsettlementcycle = settlementcycle(blmTicks.values())
        # blmrebalancingfrequency = rebalancingfrequency(blmTicks.values())
        # blmreplicationstrategy = replicationstrategy(blmTicks.values())
        # blmfundportdt = fundportdt(blmTicks.values())
        # blmtotalnumberofholdingsinport = totalnumberofholdingsinport(blmTicks.values())
        # blmnavpricingmethodology = navpricingmethodology(blmTicks.values())
        # blmfundavgdvdyld = fundavgdvdyld(blmTicks.values())
        # blmfundperatio = fundperatio(blmTicks.values())
        # blmfundpricingsource = fundpricingsource(blmTicks.values())
        # blmsixmonthavgvolume = sixmonthavgvolume(blmTicks.values())
        # blmetfnamefrombloomberg = etfnamefrombloomberg(blmTicks.values())
        # blmetfdurationfrombloomberg = etfdurationfrombloomberg(blmTicks.values())

        blmIndexes = dict()

        error_count = 0

        for mqaid, bbgtick in blmTicks.items():
            expense_ratio = 0
            mgmt_fees = 0
            index = ''
            duration = None

            try:
                if bbgtick in blmexpenseRatio:
                    if 'FUND_EXPENSE_RATIO' in blmexpenseRatio[bbgtick]:
                        expense_ratio = '%0.3f' % blmexpenseRatio[bbgtick]['FUND_EXPENSE_RATIO']

                if bbgtick in blmmanagementFee:
                    if 'FUND_MGR_STATED_FEE' in blmmanagementFee[bbgtick]:
                        mgmt_fees = '%0.3f' % blmmanagementFee[bbgtick]['FUND_MGR_STATED_FEE']

                # if bbgtick in blmbidAskSpread:
                #     if 'AVERAGE_BID_ASK_SPREAD_%' in blmbidAskSpread[bbgtick]:
                #         bid_ask_spread = '%0.4f' % blmbidAskSpread[bbgtick]['AVERAGE_BID_ASK_SPREAD_%']
                #
                # if bbgtick in blmcreateRedeemFee:
                #     if 'CREATION_FEE' in blmcreateRedeemFee[bbgtick]:
                #         createRedeemFee = '%0.4f' % blmcreateRedeemFee[bbgtick]['CREATION_FEE']
                #
                # if bbgtick in blmcreateRedeemSize:
                #     if 'FUND_CREATION_UNIT_SIZE' in blmcreateRedeemSize[bbgtick]:
                #         createRedeemSize = '%0.4f' % blmcreateRedeemSize[bbgtick]['FUND_CREATION_UNIT_SIZE']
                #
                # if bbgtick in blmetfimpliedliquidity:
                #     if 'ETF_IMPLIED_LIQUIDITY' in blmetfimpliedliquidity[bbgtick]:
                #         etf_implied_liquidity = '%0.4f' % blmetfimpliedliquidity[bbgtick]['ETF_IMPLIED_LIQUIDITY']
                #
                # if bbgtick in blmfundpctpremium:
                #     if 'FUND_PCT_PREMIUM' in blmfundpctpremium[bbgtick]:
                #         fund_pct_premium = '%0.4f' % blmfundpctpremium[bbgtick]['FUND_PCT_PREMIUM']
                #
                # if bbgtick in blmcurrencyhedgedindicator:
                #     if 'CURRENCY_HEDGED_INDICATOR' in blmcurrencyhedgedindicator[bbgtick]:
                #         currency_hedged_indicator = '%s' % blmcurrencyhedgedindicator[bbgtick]['CURRENCY_HEDGED_INDICATOR']
                #
                # if bbgtick in blmderivativesbased:
                #     if 'DERIVATIVES_BASED' in blmderivativesbased[bbgtick]:
                #         derivatives_based = '%s' % blmderivativesbased[bbgtick]['DERIVATIVES_BASED']
                #
                # if bbgtick in blmsecuritieslending:
                #     if 'SECURITIES_LENDING' in blmsecuritieslending[bbgtick]:
                #         securities_lending = '%s' % blmsecuritieslending[bbgtick]['SECURITIES_LENDING']
                #
                # if bbgtick in blmfundinceptdt:
                #     if 'FUND_INCEPT_DT' in blmfundinceptdt[bbgtick]:
                #         fund_incept_dt = '%s' % datetime.datetime.strftime(blmfundinceptdt[bbgtick]['FUND_INCEPT_DT'],
                #                                                            "%m/%d/%Y")
                #
                # if bbgtick in blmcreateredeemprocess:
                #     if 'CREATE_REDEEM_PROCESS' in blmcreateredeemprocess[bbgtick]:
                #         create_redeem_process = '%s' % blmcreateredeemprocess[bbgtick]['CREATE_REDEEM_PROCESS']
                #
                # if bbgtick in blmcreationcutofftime:
                #     if 'CREATION_CUTOFF_TIME' in blmcreationcutofftime[bbgtick]:
                #         creation_cutoff_time = '%s' % blmcreationcutofftime[bbgtick]['CREATION_CUTOFF_TIME']
                #
                # if bbgtick in blmsettlementcycle:
                #     if 'SETTLEMENT_CYCLE' in blmsettlementcycle[bbgtick]:
                #         settlement_cycle = '%s' % blmsettlementcycle[bbgtick]['SETTLEMENT_CYCLE']
                #
                # if bbgtick in blmrebalancingfrequency:
                #     if 'REBALANCING_FREQUENCY' in blmrebalancingfrequency[bbgtick]:
                #         rebalancing_frequency = '%s' % blmrebalancingfrequency[bbgtick]['REBALANCING_FREQUENCY']
                #
                # if bbgtick in blmreplicationstrategy:
                #     if 'REPLICATION_STRATEGY' in blmreplicationstrategy[bbgtick]:
                #         replication_strategy = '%s' % blmreplicationstrategy[bbgtick]['REPLICATION_STRATEGY']
                #
                # if bbgtick in blmfundportdt:
                #     if 'FUND_PORT_DT' in blmfundportdt[bbgtick]:
                #         fund_port_dt = '%s' % datetime.datetime.strftime(blmfundportdt[bbgtick]['FUND_PORT_DT'], "%m/%d/%Y")
                #
                # if bbgtick in blmtotalnumberofholdingsinport:
                #     if 'TOTAL_NUMBER_OF_HOLDINGS_IN_PORT' in blmtotalnumberofholdingsinport[bbgtick]:
                #         total_number_of_holdings_in_port = '%0.4f' % blmtotalnumberofholdingsinport[bbgtick][
                #             'TOTAL_NUMBER_OF_HOLDINGS_IN_PORT']
                #
                # if bbgtick in blmnavpricingmethodology:
                #     if 'NAV_PRICING_METHODOLOGY' in blmnavpricingmethodology[bbgtick]:
                #         nav_pricing_methodology = '%s' % blmnavpricingmethodology[bbgtick]['NAV_PRICING_METHODOLOGY']
                #
                # if bbgtick in blmfundavgdvdyld:
                #     if 'FUND_AVG_DVD_YLD' in blmfundavgdvdyld[bbgtick]:
                #         fund_avg_dvd_yld = '%0.4f' % blmfundavgdvdyld[bbgtick]['FUND_AVG_DVD_YLD']
                #
                # if bbgtick in blmfundperatio:
                #     if 'FUND_PE_RATIO' in blmfundperatio[bbgtick]:
                #         fund_pe_ratio = '%0.4f' % blmfundperatio[bbgtick]['FUND_PE_RATIO']
                #
                # if bbgtick in blmfundpricingsource:
                #     if 'FUND_PRICING_SOURCE' in blmfundpricingsource[bbgtick]:
                #         fund_pricing_source = '%s' % blmfundpricingsource[bbgtick]['FUND_PRICING_SOURCE']
                #
                # if bbgtick in blmsixmonthavgvolume:
                #     if 'VOLUME_AVG_6M' in blmsixmonthavgvolume[bbgtick]:
                #         avgvolume_6M = '%s' % blmsixmonthavgvolume[bbgtick]['VOLUME_AVG_6M']

                # if bbgtick in blmetfnamefrombloomberg:
                #     if 'NAME' in blmetfnamefrombloomberg[bbgtick]:
                #         etf_name = '%s' % blmetfnamefrombloomberg[bbgtick]['NAME'].encode('ascii', 'ignore')

                # if bbgtick in blmetfdurationfrombloomberg:
                #     if 'YAS_MOD_DUR' in blmetfdurationfrombloomberg[bbgtick]:
                #         duration = '%s' % blmetfdurationfrombloomberg[bbgtick]['YAS_MOD_DUR']

                blmIndexes[mqaid] = {'tick': bbgtick, 'mgmt_fees': mgmt_fees, 'expense_ratio': expense_ratio,
                                     # 'bid_ask_spread': bid_ask_spread,
                                     # 'create_redeem_fee': createRedeemFee, 'create_redeem_size': createRedeemSize,
                                     # 'etf_implied_liquidity': etf_implied_liquidity, 'fund_pct_premium': fund_pct_premium,
                                     # 'currency_hedged_indicator': currency_hedged_indicator,
                                     # 'derivatives_based': derivatives_based,
                                     # 'securities_lending': securities_lending, 'fund_incept_dt': fund_incept_dt,
                                     # 'create_redeem_process': create_redeem_process,
                                     # 'creation_cutoff_time': creation_cutoff_time,
                                     # 'settlement_cycle': settlement_cycle, 'rebalancing_frequency': rebalancing_frequency,
                                     # 'replication_strategy': replication_strategy, 'fund_port_dt': fund_port_dt,
                                     # 'total_number_of_holdings_in_port': total_number_of_holdings_in_port,
                                     # 'nav_pricing_methodology': nav_pricing_methodology,
                                     # 'fund_avg_dvd_yld': fund_avg_dvd_yld,
                                     # 'fund_pe_ratio': fund_pe_ratio, 'fund_pricing_source': fund_pricing_source,
                                     # 'avgvolume_6M': avgvolume_6M, 'etf_name': etf_name,
                                     # 'duration': duration
                                     }
            except Exception as e:
                error_count = error_count + 1

        if error_count > 0.01 * len(blmTicks):
            print ("Error %s" % (e))

        try:
            outFile = open(args.output, "w", newline='')
            writer = csv.writer(outFile)
            header = ['date', 'mqaid', 'ticker', 'mgmtfees', 'expense_ratio',
                      # 'bid_ask_spread_pct', 'create_redeem_fee', 'create_redeem_size',
                      # 'etf_implied_liquidity', 'fund_pct_premium', 'currency_hedged_indicator',
                      # 'derivatives_based', 'securities_lending', 'fund_incept_dt', 'create_redeem_process',
                      # 'creation_cutoff_time', 'settlement_cycle', 'rebalancing_frequency', 'replication_strategy',
                      # 'fund_port_dt', 'total_number_of_holdings_in_port', 'nav_pricing_methodology',
                      # 'fund_avg_dvd_yld', 'fund_pe_ratio', 'fund_pricing_source', 'avgvolume_6M', 'etf_name',
                      'duration']

            writer.writerow(header)
            currdate = datetime.date.today() - datetime.timedelta(1)
            for mqaid in blmIndexes:
                # Setting duration as 0(hardcoded). It is used only in D shell(data for duration again no one uses)
                row_to_write = [currdate.strftime('%m/%d/%Y'), mqaid, blmIndexes[mqaid]['tick'],
                                blmIndexes[mqaid]['mgmt_fees'], blmIndexes[mqaid]['expense_ratio'], 0]
                # blmIndexes[mqaid]['bid_ask_spread'],
                #  blmIndexes[mqaid]['create_redeem_fee'], blmIndexes[mqaid]['create_redeem_size'],
                #  blmIndexes[mqaid]['etf_implied_liquidity'], blmIndexes[mqaid]['fund_pct_premium'],
                #  blmIndexes[mqaid]['currency_hedged_indicator'], blmIndexes[mqaid]['derivatives_based'],
                #  blmIndexes[mqaid]['securities_lending'], blmIndexes[mqaid]['fund_incept_dt'],
                #  blmIndexes[mqaid]['create_redeem_process'], blmIndexes[mqaid]['creation_cutoff_time'],
                #  blmIndexes[mqaid]['settlement_cycle'], blmIndexes[mqaid]['rebalancing_frequency'],
                #  blmIndexes[mqaid]['replication_strategy'], blmIndexes[mqaid]['fund_port_dt'],
                #  blmIndexes[mqaid]['total_number_of_holdings_in_port'], blmIndexes[mqaid]['nav_pricing_methodology'],
                #  blmIndexes[mqaid]['fund_avg_dvd_yld'], blmIndexes[mqaid]['fund_pe_ratio'], blmIndexes[mqaid]['fund_pricing_source'],
                #  blmIndexes[mqaid]['avgvolume_6M'], blmIndexes[mqaid]['etf_name'], blmIndexes[mqaid]['duration']]
                writer.writerow(row_to_write)
            outFile.close()
            # print('INFO: Finished writing file %s' % args.output)
            # Also write this file into static folder to copy it from there during the delta mode.
            # And remove the existing file before copying
            if os.path.exists(static_output_file):
                os.remove(static_output_file)
            shutil.copy2(args.output, static_output_file)
        except Exception as e:
            print('Error : %s' % e)
        # print('INFO: Finished')
    else:
        if os.path.exists(static_output_file):
            # Copy the output file from static dir to output dir and change the date to previous day
            df = pd.read_csv(static_output_file, sep=',')
            df['date'] = (date.today() - timedelta(days=1)).strftime('%m/%d/%Y')
            df.to_csv(args.output, index=False, sep=',', encoding='utf-8')
        else:
            raise IOError("No file found in static folder")


if __name__ == "__main__":
    main()
    
