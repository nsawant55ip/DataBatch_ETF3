import datetime, re, os, sys
import traceback
import configparser
import subprocess as SP
import time


def TimeString():
    return datetime.datetime.now().strftime("%H:%M:%S")


def DateTimeString():
    return datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")


def UnStringDateTime(DateTimeString):
    return datetime.datetime.strptime(DateTimeString,"%m/%d/%Y %H:%M:%S")


def ParseConfig(WriteFlag,config_file,logfile,LOCALdir):
    logfile.write(' ' + TimeString() + ' Commencing parsing of batch configuration file.\n')
    Error = []
    for section, options in config_file.items():#set output value to filetocheck
        try:
            if options.get("FileToCheck") == 'output' and options.get("output"):
                options["FileToCheck"] = options.get("output")
        except:
            logfile.write(' Error parsing '+sec+' section.\n')
            Error.append(section+'-'+' Config Filepath Error')
    if len(Error) == 0:
        logfile.write(' Batch configuration file successfully parsed.\n\n')
        return WriteFlag,config_file
    else:
        logfile.write(' WriteFlag will not be written due to errors in the config file.\n\n')
        return WriteFlag.append(','.join(Error)),config_file


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
        if config[ProcessName].get('parameter') and config[ProcessName].get('parameter') != 'None':
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
    config[ProcessName]['Command'] = Command
    logfile.write(' '+datetime.datetime.now().strftime("%H:%M:%S")+' Launching '+config[ProcessName]['DisplayName']+'...\n')
    logfile.write(' Command: '+config[ProcessName]['Command']+'\n\n')
    ProcObjects[ProcessName] = SP.Popen(config[ProcessName]['Command'], stdout=SP.PIPE, stderr=SP.PIPE)
    if config[ProcessName].get('AttemptNumber'):
        attempt_boolean = config[ProcessName]['AttemptNumber'] + 1
    else:
        attempt_boolean = config['DEFAULT']['AttemptNumber'] + 1
    config[ProcessName]['AttemptNumber'] = attempt_boolean
    config[ProcessName]['StartTime'] = DateTimeString()
    return config,ProcObjects,ProcStatus,EarliestStarts


def ReceiveProcess(ProcessName, ProcessObject, config, logfile, WriteFlag, ArchiveStack, HardDriveActive,ProcessStatus,EarliestStarts,Recipients,FLAGdir):
    logfile.write(' ' + TimeString() + ' ' + config[ProcessName]['DisplayName'] + ' Process Finished: \n')
    Error = False
    if config[ProcessName]['method'] in ['MQA', 'powershell', 'Split', 'Combine']:
        err_list = list(ProcessObject.stdout) # ProcessObject.stdout is a file object that provides output from the child process. Otherwise, it is None.
        REpattern = re.compile('Error')
        for line in [elem for elem in err_list if elem.strip() != ""]:
            logfile.write('  '+str(line))
    elif config[ProcessName]['method'] == 'Python':
        REpattern = re.compile('Error')
        err_list = list(ProcessObject.stderr)
        if err_list:
            err_list.append('Error')
            logfile.write('  ' + str(err_list))
        else:
            err_list = list(ProcessObject.stdout)
            if err_list:
                for line in [elem for elem in err_list if elem.strip() != ""]:
                    logfile.write('  ' + line)
    elif config[ProcessName]['method'] in ['Stata','SAS','R']:
        plogfile = open(config[ProcessName]['logfile'],'r')
        err_list = plogfile.readlines()
        plogfile.close()
        if config[ProcessName]['method'] == 'Stata': REpattern = re.compile('r\(\d+\)')  # Matches r( one or more digits) at the start of a line.
        if config[ProcessName]['method'] == 'R': REpattern = re.compile('Error')
        if config[ProcessName]['method'] == 'SAS': REpattern = re.compile('E(RROR|rror)')  # Matches Error or ERROR.
    if 'Error' in err_list:
        Error = True
    for line in [x for x in err_list if re.match(str(REpattern),str(x)) != None]:
        logfile.write(' Error detected:\n  >> '+str(line)+'\n')
        Error = True
        break
    if config[ProcessName].get('FileToCheck') != None:
        filecheck_flag = config[ProcessName].get('FileToCheck')
    else:
        filecheck_flag = config['DEFAULT']['FileToCheck']
    if filecheck_flag != 'None' and not(Error):
        try:
            if not isinstance(filecheck_flag, list):
                files_to_check = filecheck_flag.split(',')
            else:
                files_to_check = filecheck_flag
            for each_file in files_to_check:
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
        except:
            (filepath,filename) = os.path.split(filecheck_flag)
            SendRobustEmail(Recipients,[],'Normal operation will continue.\n\nThe file size of '+filename+' will need to be checked manually against '+config[ProcessName]['ArchiveDir']+'.','WARNING: '+config[ProcessName]['DisplayName']+' files size check failed on '+os.environ.get('COMPUTERNAME','UNKNOWN'))
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            logfile.write(('-'*80)+'\n File size check for '+config[ProcessName]['DisplayName']+' failed:\n\n')
            traceback.print_exception(exceptionType, exceptionValue, exceptionTraceback,limit=None, file=logfile)
    if not(Error):
        ProcessStatus[ProcessName] = 'finished'
        config[ProcessName]['EndTime'] = DateTimeString()
        logfile.write(' No errors detected.\n')
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
        if config[ProcessName].get('Dependents') and config[ProcessName].get('Dependents') != 'None':
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


def SendRobustEmail(Recipients,Attachments,Text,Subject):
    log = open(os.path.join('c:\\', 'DataBatch_ETF_NewProject', 'Logs', 'EmailLog.txt'), 'a')
    PrimaryEmailCommand = 'c:\Python3\python c:\DataBatch_ETF_NewProject\Code\Fork_Email.py --SubjectLine=\"' + Subject + '\" --Text=\"' + Text + '\"'
    for recipient in list(set(Recipients)):
        PrimaryEmailCommand += ' --Recipients=\"' + recipient + '\"'
    for attachment in Attachments:
        PrimaryEmailCommand += ' --Attachments=\"' + attachment + '\"'
    BackupEmailCommand = PrimaryEmailCommand + ' --Username=\"AdaFetch@gmail.com\" --Password=\"gmailada\" --Server=\"smtp.gmail.com\"'
    Attempt = 1
    Finished = False
    while True:
        if Attempt % 2 == 1:
            SendProcess = SP.Popen(PrimaryEmailCommand, stdout=SP.PIPE, stderr=SP.PIPE)
        else:
            SendProcess = SP.Popen(BackupEmailCommand, stdout=SP.PIPE, stderr=SP.PIPE)
        Timer = 0
        while Timer < 300:
            if SendProcess.poll() != None: # None value indicates that the process hasn’t terminated yet. #if no error
                if re.match('Error:', ''.join(str(SendProcess.stdout))) == None:
                    log.write(' ' + datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S") + ' Email with subject \"' +
                              Subject + '\" finished on attempt number ' + str(Attempt) + '.\n')
                    log.close()
                    Finished = True
                    break
                else:
                    log.write(' ' + datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S") + ' Email with subject \"' +
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