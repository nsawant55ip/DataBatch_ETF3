import time,sys
import argparse

# Process any inputs from the command line.
parser = argparse.ArgumentParser()
parser.add_argument('-S','--Source',dest='Source', help='Source files.')
parser.add_argument('-D','--Destination',dest='Destination', help='Destination file.')
parser.add_argument('-N','--NumFiles',dest='Numfiles',type=int, help='Number of files to split into.')
parser.add_argument('-A','--Attempts',dest='Attempts',help='Max number of attempts. Default = 1.',type=int, default = 1)
parser.add_argument('-W','--Wait',dest='Wait',help='Number of seconds to wait between attempts. Default = 0.',type = int, default = 0)
args = parser.parse_args()

Finished = False
for AttemptNum in range(1,args.Attempts+1):
    try:
        f = open(args.Source,'r')
        temp = f.readlines()
        templength = len(temp)
        f.close()

        for x in range(1,args.Numfiles+1):
            OUTFILE = open(args.Destination.replace('1.',str(x)+'.'),'w')
            if x > 1:
                # Write the header line.
                OUTFILE.write(temp[0])
            for line in range(int(templength*(x-1)/(args.Numfiles)),int(templength*x/(args.Numfiles))):
                OUTFILE.write(temp[line])
            OUTFILE.close()
        Finished = True
        break
    except:    
        time.sleep(args.Wait)
    
if Finished == False:
    print('Error: Unable to split files.')

