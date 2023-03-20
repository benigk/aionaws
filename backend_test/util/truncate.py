
""" 
This script is to truncate the given number of articles in our article set
"""

import argparse
import gzip
import os
import sys


def truncate(args):

    try: 
        n = int(args.n)
    except Exception:
        print('Expected to have a number on argument "n", got {} instead'.format(type(args.n)))
        sys.exit() 

    with gzip.open(os.path.join(args.input_dir, args.input_name), 'r') as input_f:         
        line = input_f.readline()
        with gzip.open(os.path.join(args.output_dir, args.output_name), 'w') as output_f:
            while n and line:
                if b'</PubmedArticle>' in line:
                    n -= 1
                output_f.write(line)
                line = input_f.readline()
            output_f.write(b'</PubmedArticleSet>\n')
    print('Successfully truncate %d articles from file %s' % (int(args.n), args.input_name))


def main(args):
    truncate(args)

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-filename", dest="input_name", help="Path to input file")
    parser.add_argument("-o", "--output_filename", dest="output_name", help="Name for the truncated file")
    parser.add_argument("-n", "--truncated_number", dest="n", help="The number of truncated articles from the original file")
    parser.add_argument("-id", "--input_directory", dest="input_dir", default=".", help="The path to the directory of input file, the default is the current working directory")
    parser.add_argument("-od", "--output_directory", dest="output_dir", default=".", help="The path to the directory of output file, the default is the current working directory")
    
    args = parser.parse_args()
    if args.input_name and args.output_name and args.n:
        main(args)

