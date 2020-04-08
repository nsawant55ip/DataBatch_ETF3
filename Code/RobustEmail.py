# Command-line base robust email sending utility designed that forks
# off the base program. If a send process doesn't complete within five
# minutes, the process is killed and switches server (from ada to gmail, 
# or vice-versa).

# Note that this code will keep attempting to send email until success. 

import datetime, os, time, re
import argparse
import subprocess as SP

# Process any inputs from the command line.
parser = argparse.ArgumentParser()
parser.add_argument('-R', '--Recipients', dest='Recipients', action='append',
                  help='Use this to add each recipient to the email.', default=[])
parser.add_argument('-A', '--Attachments', dest='Attachments', action='append', default=[],
                  help='Use this to attach each file to the email.')
parser.add_argument('-S', '--SubjectLine', dest='Subject', help='Subject line.', default=r'')
parser.add_argument('-T', '--Text', dest='Text', help='File or string with text for email.', default=r'')
args = parser.parse_args()

log = open(os.path.join('c:\\', 'DataBatch_ETF_NewProject', 'Logs', 'EmailLog.txt'), 'a')
PrimaryEmailCommand = 'c:\Python3\python c:\DataBatch_ETF_NewProject\Code\Fork_Email.py --SubjectLine=\"' + args.Subject + '\" --Text=\"' + args.Text + '\"'
for recipient in list(set(args.Recipients)):
    PrimaryEmailCommand += ' --Recipients=\"' + recipient + '\"'
for attachment in args.Attachments:
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
                          args.Subject + '\" finished on attempt number ' + str(Attempt) + '.\n')
                log.close()
                Finished = True
                break
            else:
                log.write(' ' + datetime.datetime.now().strftime("%A %m/%d/%Y %H:%M:%S") + ' Email with subject \"' +
                          args.Subject + '\" had an error on attempt number ' + str(
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
                  args.Subject + '\" timed out on attempt number ' + str(Attempt) + '.\n')
        Attempt += 1
