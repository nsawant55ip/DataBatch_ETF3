#from email.MIMEMultipart import MIMEMultipart
#from email.MIMEText import MIMEText
#from email.MIMEBase import MIMEBase
#from email import Encoders
import argparse
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import re, sys, smtplib, os


def processOptions():
    # Process any inputs from the command line.
    parser = argparse.ArgumentParser()
    # parser.add_option('-R','--Recipients',dest='Recipients',action='append',
    #     help='Use this to add each recipient to the email.',default = [r'batch_monitor@55-ip.com'])
    parser.add_argument('-R', '--Recipients', dest='Recipients', action='append',
                        help='Use this to add each recipient to the email.', default=[r'nsawant@55-ip.com'])
    parser.add_argument('-A', '--Attachments', dest='Attachments', action='append', default=[],
                        help='Use this to attach each file to the email.')
    parser.add_argument('-L', '--SubjectLine', dest='Subject', help='Subject line.', default='testing')
    parser.add_argument('-T', '--Text', dest='Text', help='File with text for email.', default='Hello')
    parser.add_argument('-U', '--Username', dest='Username', default=r'abatch@55-ip.com',
                        help='Username. Default is abatch@55-ip')
    parser.add_argument('-P', '--Password', dest='Password', default=r'Bl!zzard152',
                        help='Password. Default is for abatch@55-ip')
    parser.add_argument('-S', '--Server', dest='Server', default=r'smtp.office365.com',
                        help='SMTP server. Default is smtp.collaborationhost.net')
    parser.add_argument('-O', '--Port', dest='Port', default=587, type=int,
                        help='Port. Default is 587')
    args = parser.parse_args()
    return args


def fork_email(recipients, attachments, text, subject):
    try:
    #if True:
        args = processOptions()
        server = smtplib.SMTP(args.Server, args.Port)
        msg = MIMEMultipart()
        msg['From'] = args.Username
        msg['To'] = ','.join(list(set(recipients)))
        msg['Subject'] = subject
        if len(text) > 0:
            if re.match('[a-zA-Z]{1}:(/|\\\\)', text) != None:
                # Email content is stored in a file.
                content = open(os.path.normpath(text), 'r')
                msg.attach(MIMEText(''.join(content.readlines())))
                content.close()
            elif "<html>" in text:
                msg.attach(MIMEText(text, "html"))
            else:
                # Email content is just a string.
                msg.attach(MIMEText(text))
        for f in attachments:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(os.path.normpath(f), "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
            msg.attach(part)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(args.Username, args.Password)
        server.sendmail(args.Username, recipients, msg.as_string())
        server.close()
        print('Email sent successfully.')
    except Exception as e:
        print("Error: '%s'" % e)


def main():
    args = processOptions()
    fork_email(args.Recipients, args.Attachments, args.Text, args.Subject)


if __name__ == "__main__":
    main()
