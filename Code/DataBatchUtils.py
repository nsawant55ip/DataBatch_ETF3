import datetime, re, os, sys
import traceback
import configparser
import subprocess as SP
from io import BytesIO
import shutil
import time
import argparse

def TimeString():
    return datetime.datetime.now().strftime("%H:%M:%S")
    
def DateTimeString():
    return datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    
def UnStringDateTime(DateTimeString):
    return datetime.datetime.strptime(DateTimeString,"%m/%d/%Y %H:%M:%S")    
    
# def ParseConfig_old(WriteFlag,Config,logfile,LOCALdir):
#     print("In Parse Config------------------")
#     logfile.write(' '+TimeString()+' Commencing parsing of batch configuration file.\n')
#     Error = []
#     # First propagate any entries from other sections and set the logfiles to write to the batch directory.
#     REpattern1 = re.compile('@,')
#     print("REpattern1------",REpattern1)
#     for sec in Config.sections():
#
#         if Config.has_option(sec,'logfile'): # check if 'logfile' is present in current section
#             print("value",os.path.join(LOCALdir,Config.get(sec,'logfile')))
#             Config.set(sec,'logfile',os.path.join(LOCALdir,Config.get(sec,'logfile'))) # set the given option to the specified value
#
#         print("sec------------------", sec)
#         for key in Config.options(sec): # Config.options(sec) gives options of section, key gets only keys of those options
#             #try:
#             if True:
#                 print("key",key)
#                 print("Config.get(sec,key)",Config.get(sec,key))
#                 if re.match(REpattern1,Config.get(sec,key)) != None: # if the pattern matches i.e @
#                     print("re.match(REpattern1,Config.get(sec,key))", re.match(REpattern1, Config.get(sec, key)))
#                     temp = Config.get(sec,key).split(',')  # split by , as coz filetocheck is written in ,
#                     print("temp",temp)
#                     Config.set(sec,key,Config.get(temp[1],temp[2])) # get the output values using temp1 and temp2
#                     print("Config.get(temp[1],temp[2])",Config.get(temp[1],temp[2]))
#             # except:
#             #     logfile.write(' Error parsing '+key+' option within '+sec+' section.\n')
#             #     Error.append(key+'-'+sec+' Config Error')
#
#
#     # Then make sure all the filepaths are kosher.
#     REpattern2 = re.compile('[a-zA-Z]{1}:(/|\\\\)')
#     for sec in Config.sections():
#         for key in Config.options(sec):
#             #try:
#             if True:
#                 if re.match(REpattern2,Config.get(sec,key)) != None: # match filepath patterns with the keys, if found, set
#                     Config.set(sec,key,os.path.normpath(Config.get(sec,key)))
#             # except:
#             #     logfile.write(' Filepath error parsing '+key+' option within '+sec+' section.\n')
#             #     Error.append(key+'-'+option+' Config Filepath Error')
#
#     if len(Error) == 0:
#         logfile.write(' Batch configuration file successfully parsed.\n\n')
#         return WriteFlag,Config
#     else:
#         logfile.write(' WriteFlag will not be written due to errors in the config file.\n\n')
#         return WriteFlag.append(','.join(Error)),Config

def ParseConfig(WriteFlag,config_file,logfile,LOCALdir):
    logfile.write(' ' + TimeString() + ' Commencing parsing of batch configuration file.\n')
    Error = []
    # for sec in Config.sections():
    #     for key in Config.options(sec):
    for section, options in config_file.items():#set output value to filetocheck
        try:
            if options.get("FileToCheck") == 'output' and options.get("output"):
                options["FileToCheck"] = options.get("output")
        except:
            logfile.write(' Error parsing '+sec+' section.\n')
            Error.append(section+'-'+' Config Filepath Error')
            # Error.append(key + '-' + sec + ' Config Error')
    if len(Error) == 0:
        logfile.write(' Batch configuration file successfully parsed.\n\n')
        return WriteFlag,config_file
    else:
        logfile.write(' WriteFlag will not be written due to errors in the config file.\n\n')
        return WriteFlag.append(','.join(Error)),config_file

# def ModConfig_old(config,logfile,BASEpath):
#     '''
#     When testing, we want to be able to move everything without changing every damned section
#     in the Config file so this lets us modify config entries on the fly.
#     02/23/2011 jvernaleo
#     '''
#     for sec in Config.sections():
#         for key in Config.options(sec):
#             value=Config.get(sec,key)
#             #Probably want a more general way to do this but is it fine for now.
#             if value.find('C:\\DataBatch_ETF_NewProject')>-1:
#                 value=value.replace("C:\\DataBatch_ETF_NewProject",BASEpath)
#                 Config.set(sec,key,value)
#     return Config


def ModConfig(config,logfile,BASEpath):
    '''
    When testing, we want to be able to move everything without changing every damned section
    in the Config file so this lets us modify config entries on the fly.
    02/23/2011 jvernaleo
    '''
    for section, options in config.items():
        for option in options:
            value = options.get(option)
            #Probably want a more general way to do this but is it fine for now.
            if value.find('C:\\DataBatch_ETF_NewProject')>-1:
                value = value.replace("C:\\DataBatch_ETF_NewProject",BASEpath)
                options["option"] = value
    return config
        
def CanLaunch(ProcessName, ProcessStatus, config, Earliest,HardDrive):
    if config[ProcessName].get('Requires') and config[ProcessName]['Requires'] != 'None':
        Precedents = config[ProcessName]['Requires'].split(',')
    else:
        Precedents = []
    if config[ProcessName].get('HardDrive'):
        harddrive_boolean = config[ProcessName]['HardDrive']
    else:
        harddrive_boolean = config['DEFAULT']['HardDrive']
    if len([elem for elem in Precedents if ProcessStatus[elem] != 'finished']) == 0 and ProcessStatus[ProcessName] == 'unlaunched' and Earliest[ProcessName] < datetime.datetime.now() and not(harddrive_boolean and HardDrive):
        return True
    else:
        return False

def LaunchProcess(ProcessName, config, logfile, ProcObjects, ProcStatus, EarliestStarts):
    if config[ProcessName]['method'] == 'powershell':
        Command = '\"powershell"  "C:\\DataBatch_ETF_NewProject\\Code\\run_sql.ps1" \"'+config[ProcessName]['query']+'\" \"'+config[ProcessName]['output']+ '\"'
    elif config[ProcessName]['method'] == 'Python':
        if config[ProcessName].get('parameter'):
            param = config[ProcessName]['parameter']
        else:
            param = ""
        Command = '\"python" \"' + config[ProcessName]['query'] + '" ' + param
    elif config[ProcessName]['method'] == 'Stata':
        Command = '"C:\Program Files\Stata9\wmpstata.exe" /e do \"'+config[ProcessName]['dofile']+'\"'
        for ArgNum in range(1,config[ProcessName]['Arguments']+1):
            if re.match('[a-zA-Z]{1}:(/|\\\\)', config[ProcessName]['Arguments'+str(ArgNum)]) != None:
                Command += ' \"'+config[ProcessName]['Arguments'+str(ArgNum)]+'\"'
            else:
                Command += ' '+config[ProcessName]['Arguments'+str(ArgNum)]
        Command += ' \"'+config[ProcessName]['logfile']+'\"'
    elif config[ProcessName]['method'] == 'R':
        try:
            Rlogfile = config[ProcessName]['logfile']
        except:
            Rlogfile = config[ProcessName]['Rscript'].upper()
            Rlogfile = config[ProcessName]['Rscript'].replace('.R','.log')
        Command = '\"C:\Program Files\R\R-3.1.2\Bin\R\" --vanilla <'+config[ProcessName]['Rscript']+'> '+Rlogfile
        try:
            if config[ProcessName].get('Arguments'):
                arguments_flag = config[ProcessName]['Arguments']
            else:
                arguments_flag = 0
            if arguments_flag > 0:
                Command += ' --args'
                for ArgNum in range(1,arguments_flag+1):
                    if re.match('[a-zA-Z]{1}:(/|\\\\)',arguments_flag+str(ArgNum)) != None:
                        Command += ' \"'+arguments_flag+str(ArgNum)+'\"'
                    else:
                        Command += ' '+arguments_flag+str(ArgNum)
        except configparser.NoOptionError:
            pass
    #Config.set(ProcessName,'Command',Command)
    config[ProcessName]['Command'] = Command
    logfile.write(' '+datetime.datetime.now().strftime("%H:%M:%S")+' Launching '+config[ProcessName]['DisplayName']+'...\n')
    logfile.write(' Command: '+config[ProcessName]['Command']+'\n\n')
    print("config[ProcessName]['Command']",config[ProcessName]['Command'])
    ProcObjects[ProcessName] = SP.Popen(config[ProcessName]['Command'], stdout=SP.PIPE, stderr=SP.PIPE)
    #Config.set(ProcessName,'AttemptNumber',Config.getint(ProcessName,'AttemptNumber') + 1)
    if config[ProcessName].get('AttemptNumber'):
        attempt_boolean = config[ProcessName]['AttemptNumber'] + 1
    else:
        attempt_boolean = config['DEFAULT']['AttemptNumber'] + 1
    config[ProcessName]['AttemptNumber'] = attempt_boolean
    #Config.set(ProcessName,'StartTime',DateTimeString())
    config[ProcessName]['StartTime'] = DateTimeString()
    return config,ProcObjects,ProcStatus,EarliestStarts




def ReceiveProcess(ProcessName, ProcessObject, config, logfile, WriteFlag, ArchiveStack, HardDriveActive,ProcessStatus,EarliestStarts,Recipients,FLAGdir):
    logfile.write(' ' + TimeString() + ' ' + config[ProcessName]['DisplayName'] + ' Process Finished: \n')
    Error = False
    #if Config.get(ProcessName,'method') in ['MQA','powershell','Split','Combine','Python']:
    if config[ProcessName]['method'] in ['MQA', 'powershell', 'Split', 'Combine']:
        # FILE = list(ProcessObject.communicate())
        #if ProcessObject.stdout
        FILE = list(ProcessObject.stdout) # ProcessObject.stdout is a file object that provides output from the child process. Otherwise, it is None.
        print("FILE",list(ProcessObject.stdout))
        REpattern = re.compile('Error')
        #else:
        #    FILE = None
        for line in [elem for elem in FILE if elem.strip() != ""]:
            logfile.write('  '+str(line))
    elif config[ProcessName]['method'] == 'Python':
        FILE = list(ProcessObject.stdout)
        REpattern = re.compile('Error')
        if FILE:
            for line in [elem for elem in FILE if elem.strip() != ""]:
                logfile.write('  ' + str(line))
        else:
            ERR_FILE = list(ProcessObject.stderr)
            if ERR_FILE:
                FILE.append('Error')
                logfile.write('  ' + str(ERR_FILE))
    elif config[ProcessName]['method'] in ['Stata','SAS','R']:
        plogfile = open(config[ProcessName]['logfile'],'r')
        FILE = plogfile.readlines()
        plogfile.close()
        if config[ProcessName]['method'] == 'Stata': REpattern = re.compile('r\(\d+\)')  # Matches r( one or more digits) at the start of a line.
        if config[ProcessName]['method'] == 'R': REpattern = re.compile('Error')
        if config[ProcessName]['method'] == 'SAS': REpattern = re.compile('E(RROR|rror)')  # Matches Error or ERROR.
#    if FILE:

    for line in [x for x in FILE if re.match(str(REpattern),str(x)) != None]:
            logfile.write(' Error detected:\n  >> '+str(line)+'\n')
            Error = True
            break

    if config[ProcessName].get('FileToCheck') != None:
        filecheck_flag = config[ProcessName].get('FileToCheck')
    else:
        filecheck_flag = config['DEFAULT']['FileToCheck']
    print("ProcessName---",ProcessName)
    if filecheck_flag != 'None' and not(Error):
        # try:
        if True:
            if not isinstance(filecheck_flag, list):
                files_to_check = filecheck_flag.split(',')
            else:
                files_to_check = filecheck_flag
            print("files_to_check",files_to_check)
            for each_file in files_to_check:
                if os.path.exists(each_file):
                    print("each_file",each_file)
                    NewFileSize = os.stat(each_file.strip()).st_size
                    (filepath,filename) = os.path.split(each_file.strip())
                    (root,ext) = os.path.splitext(filename)
                    ArchiveFiles = [elem for elem in os.listdir(config[ProcessName]['ArchiveDir']) if re.search('^'+root+'.(\d{8})'+ext+'$',elem)!=None]
                    if not ArchiveFiles:
                        #implementation_file = os.path.join("S:\Quant\qsf_etf\implementation", filename)
                        implementation_path = config[ProcessName]['ArchiveDir']
                        implementation_path = implementation_path.split('ARCHIVES')[0]
                        implementation_file = os.path.join(implementation_path, filename)
                        if os.path.exists(implementation_file):
                            LastFile = implementation_file
                    else:
                        ArchiveFiles.sort()
                        LastFile = os.path.join(config[ProcessName]['ArchiveDir'],ArchiveFiles[-1])
                    LastFileSize = os.stat(LastFile).st_size
                    if NewFileSize < 0.99*LastFileSize:
                        logfile.write(' Filesize of '+each_file.strip()+' NOT consistent with filesize of '+LastFile+'.\n')
                        logfile.write(' Filesize Error: '+each_file.strip()+' only '+str(NewFileSize)+'k compared with '+str(LastFileSize)+'k for '+LastFile+'.\n')
                        SendRobustEmail(Recipients,[],'Normal operation will continue.\n\nThe file size of '+filename+' will need to be checked against '+config[ProcessName]['ArchiveDir']+'.','WARNING: '+config[ProcessName]['DisplayName']+' files size check failed on '+os.environ.get('COMPUTERNAME','UNKNOWN'))
                        logfile.write(('-'*80)+'\n File size check for ' +config[ProcessName]['DisplayName']+' failed:\n\n')
                    else:
                        logfile.write(' Filesize of '+each_file.strip()+' consistent with filesize of '+LastFile+'.\n')
        #except:
        #     (filepath,filename) = os.path.split(filecheck_flag)
        #     SendRobustEmail(Recipients,[],'Normal operation will continue.\n\nThe file size of '+filename+' will need to be checked manually against '+config[ProcessName]['ArchiveDir']+'.','WARNING: '+config[ProcessName]['DisplayName']+' files size check failed on '+os.environ.get('COMPUTERNAME','UNKNOWN'))
        #     exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        #     logfile.write(('-'*80)+'\n File size check for '+config[ProcessName]['DisplayName']+' failed:\n\n')
        #     traceback.print_exception(exceptionType, exceptionValue, exceptionTraceback,limit=None, file=logfile)
            
    if not(Error):
        ProcessStatus[ProcessName] = 'finished'
        config[ProcessName]['EndTime'] = DateTimeString()
        logfile.write(' No errors detected.\n')
        #Hack to use a static, manually created Universe for the moment
        #This should not be done this way for long!
        #Among other things, there is no need to copy every damned time a
        #step finishes but it is faster to do this for the moment.
        #08/10/2011
        #jvernaleo
        ####################
        #logfile.write("Copy static Universe in place of the exch Universe.\n")
        #staticFile=os.path.join("E:\\","DataBatch_ETF_NewProject","US_Universe_exch_all_10pct.QAP")
        #destination=os.path.join("E:\\","DataBatch_ETF_NewProject","Output","US_Universe_exch_all.QAP")
        #shutil.copy2(staticFile,destination)
        ####################
        #if HardDriveActive & Config.getboolean(ProcessName,'HardDrive'):
        if config[ProcessName].get('HardDrive'):
            harddrive_boolean = config[ProcessName]['HardDrive']
        else:
            harddrive_boolean = config['DEFAULT']['HardDrive']
        if HardDriveActive & harddrive_boolean:
            HarDriveActive = False
        if config[ProcessName].get('Archive'):
            archive_flag = config[ProcessName]['Archive']
        else:
            archive_flag = config['DEFAULT']['Archive']
        if archive_flag != 'None':
            for x in range(archive_flag):
                ArchiveStack.append(archive_flag+str(x+1))
        if config[ProcessName]['LocalizeOut'] != 'None':
            # For outputs that get transferred back to the local data batch, write a flag for completion.
            FlagPath = os.path.join(FLAGdir,ProcessName+'_flag.txt')
            FlagLog = open(os.path.join(FLAGdir,'completeflaglog.txt'),'a')
            if os.path.isfile(FlagPath):
                Flag = open(FlagPath,'w')
                logfile.write(' '+FlagPath+' written, but already existed.')
                FlagLog.write(datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M")+' '+ProcessName+'_flag.txt written, but already existed.\n')	
            else:
                Flag = open(FlagPath,'w')
                logfile.write(' '+FlagPath+' written.\n')
                FlagLog.write(datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M")+' '+ProcessName+'_flag.txt written.\n')
            FlagLog.close()
            Flag.close() 
        logfile.write('\n')
    elif config[ProcessName]['AttemptNumber'] == 1:
        logfile.write(' '+config[ProcessName]['DisplayName']+' failed. Will re-attempt in one minute.\n\n')
        EarliestStarts[ProcessName] = datetime.datetime.now()+datetime.timedelta(minutes=1)
        ProcessStatus[ProcessName] = 'unlaunched'
    elif config[ProcessName]['AttemptNumber'] == 2:
        logfile.write(' '+config[ProcessName]['DisplayName']+' failed. Will re-attempt in ten minutes.\n\n')
        EarliestStarts[ProcessName] = datetime.datetime.now()+datetime.timedelta(minutes=10)
        ProcessStatus[ProcessName] = 'unlaunched'
    else:
        logfile.write(' '+config[ProcessName]['DisplayName']+' failed three times. WriteFlag set to false.\n\n')
        WriteFlag.append(config[ProcessName]['DisplayName']+' failed three times.')
        ProcessStatus[ProcessName] = 'finished'
        # Set any later processes to finshed.
        if config[ProcessName]['Dependents'] != 'None':
            for dependent in config[ProcessName]['Dependents'].split(','):
                ProcessStatus[dependent] = 'finished'
        SendRobustEmail(Recipients,[],config[ProcessName]['DisplayName']+' failed three times. Writeflag will not be written.',os.environ.get('COMPUTERNAME','UNKNOWN')+' Alert - '+config[ProcessName]['DisplayName'] +' failed.')
    return WriteFlag, ArchiveStack, HardDriveActive, EarliestStarts, ProcessStatus
    
def TimeOutMQA(Process,ProcessStatus,ProcessObjects,config,logfile,WriteFlag):
    StartTime = UnStringDateTime(config[Process]['StartTime'])
    (H,M,S) = config[Process]['MaxTime'].split(':')
    if datetime.datetime.now() - StartTime > datetime.timedelta(hours=int(H),minutes=int(M),seconds=int(S)):
        # The process is toast. Restart.
        ProcessObjects[Process].terminate()
        if config[Process]['AttemptNumber'] == 1:
            logfile.write(' '+TimeString()+' '+config[Process]['DisplayName']+' timed out on first attempt. Will re-attempt.\n')
            ProcessStatus[Process] = 'unlaunched'
        elif config[Process]['AttemptNumber'] == 2:
            logfile.write(' '+TimeString()+' '+config[Process]['DisplayName']+' timed out on second attempt. Will re-attempt.\n')
            ProcessStatus[Process] = 'unlaunched'
        else:
            logfile.write(' '+config[Process]['DisplayName']+' timed-out on third attempt. WriteFlag set to false.\n')
            WriteFlag.append(config[Process]['DisplayName']+' failed three times.')
            ProcessStatus[Process] = 'finished'
            # Set any later processes to finshed.
            for dependent in config[Process]['Dependents'].split(','):
                ProcessStatus[dependent] = 'finished'
    return ProcessStatus, ProcessObjects, config, WriteFlag
    
def LaunchArchive(ArchiveName,config,logfile,Stamp):
    ArchiveCommand = '\"C:\\Python3\\python C:\\DataBatch_ETF_NewProject\\Code\\Fork_Archive.py -S '+Stamp+' -A 3 -W 5 -D '+config[ProcessName]['Dependents']
    for option in config[ProcName]['Archive'].split(','): ArchiveCommand += ' -F '+config[ProcName][option]
    logfile.write(' '+TimeString()+' Launching archiving for '+config[ArchiveName]['DisplayName']+'.\n')
    logfile.write(' Command: '+ArchiveCommand+'\n\n')
    ArchiveObject = SP.Popen(ArchiveCommand, stdout=SP.PIPE, stderr=SP.PIPE)
    return ArchiveObject
    
def ReceiveArchive(ArchiveName,ArchiveObject,config,logfile):
    Error = False
    FILE = list(ArchiveObject.stdout)
    REpattern = re.compile('Error')
    for line in [x for x in FILE if x.strip() != ""]:
        logfile.write('  '+line)
    for line in [x for x in FILE if re.match(REpattern,x) != None]:
        logfile.write(' '+TimeString+' Archiving of output from '+config[ArchiveName]['DisplayName']+' failed. Complete Flag unaffected.\n\n')
        Error = True
        break
    if not(Error): logfile.write(' '+TimeString+' Archiving of output from '+config[ArchiveName]['DisplayName']+' finished.\n\n')
    return
 
# def SendRobustEmail_old(Recipients,Attachments,Text,Subject):
#     Command = 'C:\\Python3\python C:\\DataBatch_ETF_NewProject\\Code\\RobustEmail.py --SubjectLine=\"'+Subject+'\" -T \"'+Text+'\"'
#     for recipient in Recipients: Command += ' -R '+recipient
#     for attachment in Attachments: Command += ' -A '+attachment
#     EmailObject = SP.Popen(Command, stdout=SP.PIPE, stderr=SP.PIPE)
#     return EmailObject, Subject

def SendRobustEmail(Recipients,Attachments,Text,Subject):
    log = open(os.path.join('c:\\', 'DataBatch_ETF_NewProject', 'Logs', 'EmailLog.txt'), 'a')
    PrimaryEmailCommand = 'c:\Python3\python c:\DataBatch_ETF_NewProject\Code\Fork_Email.py --SubjectLine=\"' + Subject + '\" --Text=\"' + Text + '\"'
    for recipient in Recipients:
        PrimaryEmailCommand += ' --Recipients=\"' + recipient + '\"'
    for attachment in Attachments:
        PrimaryEmailCommand += ' --Attachments=\"' + attachment + '\"'
    BackupEmailCommand = PrimaryEmailCommand + ' --Username=\"AdaFetch@gmail.com\" --Password=\"gmailada\" --Server=\"smtp.gmail.com\"'
    Attempt = 1
    Finished = False
    while True:
        if Attempt % 2 == 1:
            SendProcess = SP.Popen(PrimaryEmailCommand, stdout=SP.PIPE, stderr=SP.PIPE)
            command = PrimaryEmailCommand
        else:
            SendProcess = SP.Popen(BackupEmailCommand, stdout=SP.PIPE, stderr=SP.PIPE)
            command = BackupEmailCommand
        Timer = 0
        while Timer < 300:
            if SendProcess.poll() != None:  # None value indicates that the process hasn’t terminated yet. #if no error
                if re.match('Error:', ''.join(str(SendProcess.stdout))) == None:
                    log.write(
                        ' ' + datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S") + ' Email with subject \"' +
                        Subject + '\" finished on attempt number ' + str(Attempt) + '.\n')
                    log.close()
                    Finished = True
                    break
                else:
                    log.write(
                        ' ' + datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S") + ' Email with subject \"' +
                        Subject + '\" had an error on attempt number ' + str(
                            Attempt) + '. Message: ' + ''.join(SendProcess.stdout) + '\n')
                    break
            else:
                time.sleep(1)
                Timer += 1
        if Finished:
            break
        else:
            SendProcess.kill()
            log.write(' ' + datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S") + ' Email with subject \"' +
                      Subject + '\" timed out on attempt number ' + str(Attempt) + '.\n')
            Attempt += 1
    EmailObject = SP.Popen(command, stdout=SP.PIPE, stderr=SP.PIPE)
    return EmailObject, Subject
    
def CheckEmail(EmailObject,Subject,logfile):
    logfile.write(' '+TimeString()+' '+EmailObject.stdout+'\n Subject Line: '+Subject+'\n\n')

