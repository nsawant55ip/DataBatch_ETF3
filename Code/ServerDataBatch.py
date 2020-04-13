"""

File: ServerDDataBatch_ETF.py
Company: Ada Investments
Date: 08/06/2009 jspitzer
      12/06/2010 major cleanup for testing option, jvernaleo

This is the main program for the DataBatch_ETF_NewProject which runs on one of our
MarketQA servers.  It uses the qalproc command line tool to run queries which
are then accessed by the LocalBatch on each batch machine.

This requires Python for Windows 2.6

"""

import os, datetime, sys, time
import traceback
import argparse
import DataBatchUtils as DBU
import configparser
import subprocess as SP
import yaml

TeamEmails = ['batch_monitor@55-ip.com']
BASEpath = os.path.join('C:\\', 'DataBatch_ETF_NewProject')
FLAGdir = os.path.join(BASEpath, 'Flags')
LOGdir = os.path.join(BASEpath, 'Logs')

# Process any inputs from the command line.
parser = argparse.ArgumentParser()
parser.add_argument("-C", "--Config", dest="Config", default="C:\\DataBatch_ETF_NewProject\\Config\\DataBatchConfig.yml",
                    help="Configuration file. Default file is C:\\DataBatch_ETF_NewProject\\Config\\DataBatchConfig.yml")
parser.add_argument("-R", "--RunMode", dest="RunMode", default='TEST',
                    help="'TEST' mode won't touch the CompleteFlag or Archive.")
parser.add_argument("-D", "--NoDailyPricing", action="store_false", dest="ExecuteDailyPricing",
                    default=True, help="Use this flag to not run the daily pricing part.")
parser.add_argument("-B", "--NoBetas", action="store_false", dest="ExecuteBetas",
                    default=True, help="Use this flag to not run the betas estimation part.")
parser.add_argument("-M", "--NoMainBatch", action="store_false", dest="ExecuteMainBatch",
                    default=True, help="Use this flag to not run the main batch part.")
parser.add_argument("-G", "--NoSegments", action="store_false", dest="ExecuteSegments",
                    default=True, help="Use this flag to not run the Compustat Segments part.")
parser.add_argument("-A", "--NoArchive", action="store_false", dest="MakeArchive",
                    default=True, help="Use this flag to not archive output.")
parser.add_argument("-T", "--Timeless", action="store_false", dest="UseTimes",
                    default=True, help="Use this flag to ignore start times.")
parser.add_argument("-L", "--Late", action="store_true", dest="Late",
                    default=False, help="Supply this argument if commencing run after midnight.")
parser.add_argument("-O", "--Output", action="store_false", dest="Output", default=True,
                    help="Supply this argument you do not want the output dir to be cleared before commence.")
args = parser.parse_args()

args.Config = os.path.join(BASEpath, 'Config', 'DataBatchConfig.yml')
if args.RunMode == 'TEST':
    RunMode = 'TEST'
else:
    RunMode = ''
if not os.path.exists(FLAGdir):
    os.mkdir(FLAGdir)
if not os.path.exists(LOGdir):
    os.mkdir(LOGdir)

# Set up a new folder in logs dir to store logs, timings etc.
BatchID = 'Data_' + datetime.datetime.now().strftime("%Y%m%d_%H%M")
LOCALdir = os.path.join(LOGdir, BatchID)
os.mkdir(LOCALdir)

# Start a log file.
logfile = open(os.path.join(LOCALdir, 'log.txt'), 'w')
logfile.write('\n Data Batch Log File\n\n')
logfile.write(' Script initiated ' + datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S") + ' on machine ' +
                  os.environ.get('COMPUTERNAME', 'UNKNOWN') + ' logged in as ' + os.environ.get('USERNAME',
                                                                                                'UNKNOWN') + '\n\n')
if RunMode == 'TEST':
    logfile.write(' Test status on - batch info will be written to ' + LOCALdir + '.\n')
else:
    logfile.write(' Batch info will be written to ' + LOCALdir + '.\n')

# Put a try,except around everything so that errors will at least be written to the logfile.
try:
    # Work out what's going to be running and log it.
    ToExecute = ['MainBatch', 'Segments', 'Betas', 'DailyPricing']
    if args.ExecuteMainBatch == False:
        logfile.write(' Not running Main Batch.\n')
        ToExecute.remove('MainBatch')
    else:
        logfile.write(' Running Main Batch.\n')
    if args.ExecuteSegments == False:
        logfile.write(' Not running Compustat Segments.\n')
        ToExecute.remove('Segments')
    else:
        logfile.write(' Running Compustat Segments.\n')
    if args.ExecuteBetas == False:
        logfile.write(' Not running Update Betas.\n')
        ToExecute.remove('Betas')
    else:
        logfile.write(' Running Update Betas.\n')
    if args.ExecuteDailyPricing == False:
        logfile.write(' Not running Daily Pricing.\n\n')
        ToExecute.remove('DailyPricing')
    else:
        logfile.write(' Running Daily Pricing.\n\n')

    #   If not in TEST mode, if completeflag.txt exists, delete it so the analysis batch knows that
    #   the data batch is running.
    if RunMode != 'TEST':
        CompleteFlagPath = os.path.join(FLAGdir, 'CompleteFlag.txt')
        FlagLog = open(os.path.join(FLAGdir, 'CompleteFlaglog.txt'), 'a')
        if os.path.isfile(CompleteFlagPath):
            os.unlink(CompleteFlagPath)
            logfile.write(' ' + FLAGdir + '\\CompleteFlag.txt deleted.\n\n')
            FlagLog.write(datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M") + ' CompleteFlag.txt deleted.\n')
        else:
            logfile.write(' ' + FLAGdir + '\\CompleteFlag.txt not found.\n\n')
            FlagLog.write(datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M") + ' CompleteFlag.txt not found.\n')
        FlagLog.close()
        WriteFlag = []
    else:
        WriteFlag = ['Test mode.']

    #Parse the configuration file, overwriting with command line arguments where necessary.
    # Config = configparser.RawConfigParser()
    # Config.read(os.path.normpath(args.Config))
    # Config.write(open(os.path.join(LOCALdir, 'StartConfig.cfg'), 'w'))
    # (WriteFlag, Config) = DBU.ParseConfig(WriteFlag, Config, logfile, LOCALdir)
    # # When testing, change the path of the files pointed to in the config file.
    # if RunMode == 'TEST':
    #     logfile.write(' Test status on - Queries will be read from and written to ' + BASEpath + '.\n')
    #     (Config) = DBU.ModConfig(Config, logfile, BASEpath)
    # Config.write(open(os.path.join(LOCALdir, 'ParseConfig.cfg'), 'w'))

    # Parse the configuration file, overwriting with command line arguments where necessary.
    #with open(args.Config) as file:
    with open("C:\DataBatch_ETF_NewProject\Config\DataBatchConfig.yml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    with open(os.path.join(LOCALdir, 'StartConfig.yml'), 'w') as file:
        start_config = yaml.dump(config, file)
    (WriteFlag, config) = DBU.ParseConfig(WriteFlag, config, logfile, LOCALdir)
    with open(os.path.join(LOCALdir, 'ParseConfig.yml'), 'w') as file:
        parse_config = yaml.dump(config, file)


    # Send the log to the data batch primary admin.
    # DBU.SendRobustEmail(Recipients, Attachments, Text, Subject)
    logfile.flush()
    DBU.SendRobustEmail(TeamEmails,  # Recipients
                        [],  # Attachments
                        os.path.join(LOCALdir, 'log.txt'),  # Text
                        os.environ.get('COMPUTERNAME', 'UNKNOWN') + ' ETF Databatch Launched')  # Subject

    # Check whether we have access to the network drives where the archives are kept.
    #Unaccessible = [elem for elem in Config.sections() if Config.get(elem, 'Archive') != 'None' and not (os.path.exists(Config.get(elem, 'ArchiveDir')))]

    # Check whether we have access to the network drives where the archives are kept.
    # for section, options in config_file.items():
    #     if options.get('Archive') != 'None' and not (os.path.exists(options.get('ArchiveDir')))
    #         Unaccessible = [section]
    Unaccessible = [section for section,options in config.items() if options.get('Archive') != 'None' and not (os.path.exists(options.get('ArchiveDir')))]

    if len(Unaccessible) > 0:
        message = ' ' + DBU.TimeString() + ' Unable to access the following archive directories:\n'
        for Process in Unaccessible:
            message += '  ' + config[Process]['ArchiveDir'] + '\n'
        DBU.SendRobustEmail(TeamEmails, [], message, 'Network Drive Issue on ' + os.environ.get('COMPUTERNAME', 'UNKNOWN'))
    else:
        logfile.write(' ' + DBU.TimeString() + ' All network drive locations successfully accessed.\n')
    logfile.write('\n')
    OUTdir = os.path.join(BASEpath, 'Output')
    if not os.path.exists(OUTdir):
        os.mkdir(OUTdir)

    # Clear out the output directory.
    for file in os.listdir(OUTdir):
        os.unlink(os.path.join(OUTdir, file))
    logfile.write(' ' + DBU.TimeString() + ' All files in ' + OUTdir + ' deleted.\n\n')

    # Overwrite archiving instructions if archiving is turned off.
    # if args.MakeArchive == False:
    #     for sec in Config.sections():
    #         Config.set(sec, 'Archive', False)

    if args.MakeArchive == False:
        for section, options in config.items():
            options["Archive"] = False

    # Delete any flags for each file to be transferred to the local.
    logfile.write(' ' + DBU.TimeString() + ' Delete any existing subsection flags:\n')
    FlagLog = open(os.path.join(FLAGdir, 'completeflaglog.txt'), 'a')
    #for Process in [elem for elem in Config.sections() if Config.get(elem, 'LocalizeOut') != 'None']:
    for Process in [section for section, options in config.items() if config[section]['LocalizeOut'] != 'None']:
        FlagPath = os.path.join(FLAGdir, Process + '_flag.txt')
        if os.path.isfile(FlagPath):
            os.unlink(FlagPath)
            logfile.write('  ' + FlagPath + ' deleted.\n')
            FlagLog.write(
                datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M") + ' ' + Process + '_flag.txt deleted.\n')
        else:
            logfile.write('  ' + FlagPath + ' not found, deletion skipped.\n')
            FlagLog.write(datetime.datetime.now().strftime(
                "%A %m/%d/%Y %H:%M") + ' ' + Process + '_flag.txt not found, deletion skipped.\n')
    FlagLog.close()


    # Parse or overwrite timing instructions from config file. Be careful with
    # anything scheduled for after midnight, need to roll the date forward. Use
    # noon as a cutoff between stuff that runs today and yesterday - revisit if
    # the data process becomes longer.
    Date = datetime.datetime.now()
    if args.Late:
        Date = Date - datetime.timedelta(days=1)
    #EarliestStarts = dict.fromkeys(Config.sections(), Date)
    EarliestStarts = dict.fromkeys(config.keys(), Date)
    if args.UseTimes:
        #for sec in Config.sections():
        for section, options in config.items():
            #TargetTime = Config.get(sec, 'EarliestStart').split(':')
            TargetTime = config[section]['EarliestStart'].split(':')
            if int(TargetTime[0]) > 12:
                # Will run later today.
                EarliestStarts[sec] = Date.replace(hour=int(TargetTime[0]), minute=int(TargetTime[1]),
                                                   second=int(TargetTime[2]), microsecond=0)
            else:
                # Will run tomorrow, so roll the date forward to the next day.
                EarliestStarts[sec] = (Date + datetime.timedelta(days=1)).replace(hour=int(TargetTime[0]),
                                                                                  minute=int(TargetTime[1]),
                                                                                  second=int(TargetTime[2]),
                                                                                  microsecond=0)

    logfile.write('\n Processes Scheduled:\n')
    for key in list(EarliestStarts.keys()):
        logfile.write(' ' + key + ' - ' + EarliestStarts[key].strftime('%Y-%m-%d %H:%M:%S') + '\n')
    logfile.write('\n')

    # Set up a dictionary of process objects and statuses.
    # ProcObjects = dict.fromkeys(Config.sections(), None)
    # ProcStatus = dict.fromkeys(Config.sections(), 'finished')
    ProcObjects = dict.fromkeys(config.keys(), None)
    ProcStatus = dict.fromkeys(config.keys(), 'finished')
    ProcObjects.pop('DEFAULT')
    ProcStatus.pop('DEFAULT')
    # for sec in [elem for elem in ProcStatus.keys() if Config.get(elem, 'BatchSection') in ToExecute]:
    for sec in [elem for elem in ProcStatus.keys() if config[elem]['BatchSection']  in ToExecute]:
        ProcStatus[sec] = 'unlaunched'
    HardDriveActive = False  # Flag to help manage the hard drive activity.
    ArchiveObject = None
    ArchiveStack = []

    logfile.write(' ' + DBU.TimeString() + ' Commencing process launching...\n\n')

    while True:

        # Receive any processes that have finished.
        for Process in [elem for elem in list(ProcStatus.keys()) if ProcStatus[elem] == 'launched' and ProcObjects[elem].poll() != None]:
            (WriteFlag, ArchiveStack, HardDriveActive, EarliestStarts, ProcStatus) = DBU.ReceiveProcess(Process,
                                                                                                        ProcObjects[
                                                                                                            Process],
                                                                                                        config, logfile,
                                                                                                        WriteFlag,
                                                                                                        ArchiveStack,
                                                                                                        HardDriveActive,
                                                                                                        ProcStatus,
                                                                                                        EarliestStarts,
                                                                                                        TeamEmails,
                                                                                                        FLAGdir)

        # Kill any MQA queries that have run longer than expected.
        for Process in [elem for elem in list(ProcStatus.keys()) if ProcStatus[elem] == 'launched' and ProcObjects[elem].poll() == None and config[elem]['method'] == 'MQA']:
            (ProcessStatus, ProcessObjects, config, WriteFlag) = DBU.TimeOutMQA(Process, ProcStatus, ProcObjects,
                                                                                config, logfile, WriteFlag)

        # Check any running archiving processes.
        if ArchiveObject != None and ArchiveObject.poll() != None:
            DBU.ReceiveArchive(ArchiveName, ArchiveObject, config, logfile)
            ArchiveObject = ArchiveName = None
            HardDriveActive = False

        # Launch any processes that are ready to go (if there are no MQA processes running with a wait flag).
        if len([elem for elem in list(ProcStatus.keys()) if ProcStatus[elem] == 'launched' and config[elem]['method'] == 'MQA' and config[elem]['wait']]) == 0:
            for Process in [elem for elem in list(ProcStatus.keys()) if ProcStatus[elem] == 'unlaunched']:
                # Need to break out this if statement so that is the hardriveactive flag being false doesn't release all hard drive intensive processes.
                if DBU.CanLaunch(Process, ProcStatus, config, EarliestStarts, HardDriveActive):
                    (config, ProcObjects, ProcStatus, EarliestStarts) = DBU.LaunchProcess(Process, config, logfile,
                                                                                          ProcObjects, ProcStatus,
                                                                                          EarliestStarts)
                    ProcStatus[Process] = 'launched'

        # ArchiveStack will only have entries that want to be archived.
        if not (HardDriveActive) and len(ArchiveStack) > 0:
            ArchiveName = ArchiveStack.pop()
            ArchiveObject = DBU.LaunchArchive(ArchiveName, config, logfile, BatchID[5:13])
            HardDriveActive = True

        # Check everything has finished    
        if len([elem for elem in list(ProcStatus.values()) if elem != 'finished']) > 0 or len(ArchiveStack) > 0:
            #print(DBU.TimeString() + '..Still Running...')
            time.sleep(1)
        else:
            logfile.write(' ' + DBU.TimeString() + ' Last of the data batch processes finished.\n\n')
            break

            # Write the complete flag.
    if RunMode == 'TEST':
        logfile.write(' Test mode on - CompleteFlag.txt unaltered.\n')
        for i in range(0, len(WriteFlag)):
            logfile.write('   ' + WriteFlag[i] + '\n')
        EmailSubject = 'Test ETF Data Batch Log'
    else:
        # Deal with the completion flag.
        if len(WriteFlag) == 0:
            # Write the CompleteFlag.txt file whose existence tells the analysis batch to kick off.
            CompleteFlagPath = os.path.join(FLAGdir, 'completeflag.txt')
            FlagLog = open(os.path.join(FLAGdir, 'completeflaglog.txt'), 'a')
            if os.path.isfile(CompleteFlagPath):
                CompleteFlag = open(CompleteFlagPath, 'w')
                logfile.write('\n ' + DBU.TimeString() + ' ' + CompleteFlagPath + ' written, but already existed.')
                FlagLog.write(datetime.datetime.now().strftime(
                    "%A %m/%d/%Y %H:%M") + ' CompleteFlag.txt written, but already existed.\n')
            else:
                CompleteFlag = open(CompleteFlagPath, 'w')
                logfile.write(' ' + DBU.TimeString() + ' ' + CompleteFlagPath + ' written.\n')
                FlagLog.write(datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M") + ' CompleteFlag.txt written.\n')
            FlagLog.close()
            CompleteFlag.close()
            EmailSubject = ' Successful ETF Data Batch Log'
        else:
            logfile.write('\n Complete flag not written:\n')
            for reasons in WriteFlag:
                logfile.write('   ' + reasons + '\n')
            EmailSubject = ' Failed ETF Data Batch Log'

            # Write out the timings.
    TimingsFile = open(os.path.join(LOCALdir, 'Timings.csv'), 'w')
    TimingsFile.write('Process,Start Time, End Time, Duration, Attempts\n')

    for Process in config.keys() :
        if Process != 'DEFAULT':
            if config[Process].get('AttemptNumber'):
                attempt_number = config[Process].get('AttemptNumber')
            else:
                attempt_number = config['DEFAULT']['AttemptNumber']
            TimingsFile.write(','.join(
                [config[Process]['DisplayName'], config[Process]['StartTime'], config[Process]['EndTime'], str(
                DBU.UnStringDateTime(config[Process]['EndTime']) - DBU.UnStringDateTime(
                    config[Process]['StartTime'])), str(attempt_number)]) + '\n')

    TimingsFile.close()

    # Send the log to the data batch primary admin.
    logfile.flush()
    DBU.SendRobustEmail(TeamEmails, [os.path.join(LOCALdir, 'Timings.csv')], os.path.join(LOCALdir, 'log.txt'),
                        os.environ.get('COMPUTERNAME', 'UNKNOWN') + EmailSubject)

    #Config.write(open(os.path.join(LOCALdir, 'EndConfig.cfg'), 'w'))
    with open(os.path.join(LOCALdir, 'EndConfig.yml'), 'w') as file:
        end_config = yaml.dump(config, file)

    # Archive the output - but wait twenty minutes to avoid file copy messing with zipping.
    time.sleep(1200)
    ARCHIVEdir = os.path.join(BASEpath, 'archives')
    if not os.path.exists(ARCHIVEdir):
        os.mkdir(ARCHIVEdir)
    ZipCommand = '\"c:\\Program Files\\7-Zip\\7z.exe\" a \"' + ARCHIVEdir + '\\Output_' + BatchID + '.7z\" ' + OUTdir + '\\*.*\"'
    logfile.write('\n ' + DBU.TimeString() + ' Commencing Zipping...')
    logfile.write('\n Command: ' + ZipCommand + '\n')
    ZipProcess = SP.Popen(ZipCommand, stdout=SP.PIPE, stderr=SP.PIPE)
    ZipProcess.wait()
    if ZipProcess.poll() == 0:
        logfile.write('\n ' + DBU.TimeString() + ' Zipping finished successfully.\n')
    else:
        logfile.write('\n ' + DBU.TimeString() + ' Zipping failed with return code of ' + ZipProcess.poll() + '.\n')
    logfile.write('\n ' + DBU.TimeString() + ' ETF Data Batch Script finished.\n')
    logfile.close()

except:
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    logfile.write(('-'*80)+'\n Python error:\n\n')
    traceback.print_exception(exceptionType, exceptionValue, exceptionTraceback,limit=None, file=logfile)
    logfile.write('\n End of program execution ('+datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S")+').\n'+('-'*80)+'\n')
    logfile.close
    print('Exception raised.\n')
    logfile.flush()
    if RunMode == 'TEST':
        DBU.SendRobustEmail(TeamEmails,[],os.path.join(LOCALdir,'log.txt'),os.environ.get('COMPUTERNAME','UNKNOWN')+' Server ETF Data Batch Python Error.')
    else:
        DBU.SendRobustEmail(TeamEmails,[],os.path.join(LOCALdir,'log.txt'),os.environ.get('COMPUTERNAME','UNKNOWN')+' Server ETF Data Batch Python Error.')
print(DBU.TimeString() + ' Finished.')
