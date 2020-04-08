# Simple script to initiate the data batch at a certain time each day. If this 
# program is run after the specified CommenceTime on the actual day that the batch will run,
# the batch will be initiated immediately.

# JS 5/2/2009

import os,time,datetime,traceback,sys,calendar,ConfigParser
import subprocess as SP
from sys import stdout

# Parse the configuration file.
Config = ConfigParser.RawConfigParser()
Config.read(os.path.join('C:\\','DataBatch_ETF_NewProject','Config','SchedulerConfig.cfg'))

if Config.get('SCHEDULER','DayNoRun') == 'None':
    DayNoRun = []
else:
    DayNoRun = Config.get('SCHEDULER','DayNoRun').split(',')  
    DayNoRun = [int(elem) for elem in DayNoRun]

logfile = open(os.path.join('C:\\','DataBatch_ETF_NewProject','Scheduler','SchedulerLog.txt'),'a')
logfile.write('\n Scheduler initiated '+datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S")+' on machine '+
    os.environ.get('COMPUTERNAME','UNKNOWN')+' logged in as '+os.environ.get('USERNAME','UNKNOWN')+'.\n')
Message = ' Data Batch scheduled for '+Config.get('SCHEDULER','CommenceTime')+' each weekday'
if len(DayNoRun) > 1:
    DayNoRun.sort()
    Message += ' except for '+', '.join([calendar.day_name[elem] for elem in DayNoRun[0:-1]])+' and '+calendar.day_name[DayNoRun[-1]]
elif len(DayNoRun) == 1:
    Message += ' except for '+calendar.day_name[DayNoRun[-1]]
Message+='.\n'
logfile.write(Message)
print '\n'+Message+'\n Command: '+Config.get('SCHEDULER','BatchCommand')+'\n\n Warning: Closing this window will disable the Data Batch.\n'
logfile.write(' Command:'+Config.get('SCHEDULER','BatchCommand')+'\n') 

CommenceAfter = datetime.datetime.now()
while CommenceAfter.weekday() in DayNoRun:
    CommenceAfter = CommenceAfter + datetime.timedelta(days=1)
TargetTime = Config.get('SCHEDULER','CommenceTime').split(':')
CommenceAfter = CommenceAfter.replace(hour=int(TargetTime[0]),minute=int(TargetTime[1]),second=int(TargetTime[2]),microsecond=0)
if datetime.datetime.now() < CommenceAfter:
    Message = ' First run scheduled to commence '+CommenceAfter.strftime("%A %m/%d/%Y %H:%M:%S")+'.'
    logfile.write(Message+'\n')
    stdout.write(Message)
else:
    stdout.write(' ')
logfile.write('\n')
    
while True:
    if datetime.datetime.now() > CommenceAfter:
        CommenceAfter = CommenceAfter + datetime.timedelta(days=1)
        CommenceAfter = CommenceAfter.replace(hour=int(TargetTime[0]),minute=int(TargetTime[1]),second=int(TargetTime[2]),microsecond=0)
        while CommenceAfter.weekday() in DayNoRun:
            CommenceAfter = CommenceAfter + datetime.timedelta(days=1)
        logfile.write(' '+datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S")+' Launching Data Batch...\n')
        try:
            ProcessLog = open(os.path.join('C:\\','DataBatch_ETF_NewProject','Scheduler','CurrentProcessLog.txt'),'w')
            DDataBatch_ETFObject = SP.Popen(Config.get('SCHEDULER','BatchCommand'),stdout=ProcessLog, stderr=ProcessLog)
            stdout.write('\r'+79*' '+'\r Data Batch Now Running')
            DDataBatch_ETFObject.wait()
            ProcessLog.close()
            stdout.write('\r'+79*' '+'\r Next run scheduled for '+CommenceAfter.strftime("%A %m/%d/%Y %H:%M:%S")+'.')
            logfile.write(' '+datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S")+' Data Batch Finished. Next run scheduled for '+CommenceAfter.strftime("%A %m/%d/%Y %H:%M:%S")+'.\n')
        except:
            logfile.write(' '+datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S")+' Data Batch Failed:\n')
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            logfile.write(('-'*80)+'\n Python error:\n\n')
            traceback.print_exception(exceptionType, exceptionValue, exceptionTraceback,limit=None, file=logfile)
            logfile.write(' Next run scheduled for '+CommenceAfter.strftime("%A %m/%d/%Y %H:%M:%S")+'.\n')
            stdout.write('\r'+79*' '+'\r Last Batch Failed. Next run scheduled for '+datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S")+'.')
    else:
        time.sleep(60)
