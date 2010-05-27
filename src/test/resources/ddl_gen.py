#!/usr/bin/env python

import sys
from optparse import OptionParser

def generate_integer_columns(file, count):
    
    for i in range(0, int(count)):
        f.write('`field'+str(i)+'` int(11) NOT NULL default \'0\',\n')
        
    return;

if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                     help="write report to FILE", metavar="FILE")
    parser.add_option("-c", "--count", dest="count", 
                      help="number of columns")
    (options, args) = parser.parse_args()
    print "options" , options, " args: ", args
    print options.filename, options.count
    try:
        f = open(options.filename, 'w+')
    except Exception as e:
        print e
        sys.exit(1)
    else:
       generate_integer_columns(f, options.count)


