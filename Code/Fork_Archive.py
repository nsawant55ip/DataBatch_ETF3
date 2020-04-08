import shutil,time,os
import argparse

# Process any inputs from the command line.
parser = argparse.ArgumentParser()
parser.add_argument('-F','--Files',dest='Files', action = 'append', help='Files to be archived.')
parser.add_argument('-D','--Directory',dest='Directory', help='Directory to write archives to.')
parser.add_argument('-S','--Stamp',dest='Stamp',help='DateStamp to identify archive.')
parser.add_argument('-A','--Attempts',dest='Attempts',help='Max number of attempts. Default = 1.', type=int, default = 1)
parser.add_argument('-W','--Wait',dest='Wait',help='Number of seconds to wait between attempts. Default = 0.', type = int, default = 0)
args = parser.parse_args()

for file in args.Files:
    file = os.path.normpath(file)
    (root,ext) = os.path.splitext(os.path.basename(file))
    dest = os.path.join(args.Directory,root+'.'+args.Stamp+ext)
    Finished = False
    for AttemptNum in range(1,args.Attempts+1):
        try:
            shutil.copy2(file,dest)
            print(' '+file+' copied to '+dest+' successfully on attempt number '+str(AttemptNum)+'.\n')
            Finished = True
            break
        except:
            time.sleep(args.Wait)
    if Finished == False:
        print('Error: Unable to complete archiving of '+file+' to '+dest+'.\n')
