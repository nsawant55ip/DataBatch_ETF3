import shutil,time,sys
import argparse

# Process any inputs from the command line.
parser = argparse.ArgumentParser()
parser.add_argument('-S','--Source',dest='Source', help='Source files.',action='append')
parser.add_argument('-D','--Destination',dest='Destination', help='Destionation file.')
parser.add_argument('-H','--Header',dest='Header',action = "store_true", help ='Provide if files have header.')
parser.add_argument('-A','--Attempts',dest='Attempts',help='Max number of attempts. Default = 1.',type=int, default = 1)
parser.add_argument('-W','--Wait',dest='Wait',help='Number of seconds to wait between attempts. Default = 0.',type = int, default = 0)
args = parser.parse_args()

Finished = False
for AttemptNum in range(1,args.Attempts+1):
    try:
        if args.Destination != args.Source[0]:
            shutil.copy2(args.Source[0], args.Destination)
        MAINFILE = open(args.Destination, 'a')
              
        for x in range(1,len(args.Source)):
            APPENDFILE = open(args.Source[x],'r')
            if args.Header: APPENDFILE.readline()  # Skip the header line.
            for line in APPENDFILE:
                MAINFILE.write(line)
            APPENDFILE.close()
        
        MAINFILE.close()
        Finished = True
        break
    except:
        time.sleep(args.Wait)
        
if Finished == False:
    print('Error: Unable to combine files.')
else:
    print('Files combined successfully on attempt number '+str(AttemptNum)+'.')
