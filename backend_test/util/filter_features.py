
"""
This script is to filter the given features from xml
The file is supposed to be in the format of xml
"""


import argparse
import shutil
import sys


def filter_feature(args, tag):
    start_tag = ''.join(['<', tag])
    end_tag = ''.join(['</', tag, '>'])
    with open(args.input_name, 'r') as input_f:
        tmp_outname = '.'.join([args.input_name, 'cache'])
        line = input_f.readline()
        recording = True
        with open(tmp_outname, 'w') as output_f:
            while line:
                if line.lstrip().startswith(start_tag):
                    recording = False
                if line.strip().endswith(end_tag):
                    recording = True
                    line = input_f.readline()
                    continue
                if recording:
                    output_f.write(line)
                line = input_f.readline()
        shutil.move(tmp_outname, args.input_name)     


def main(args):
    if args.tags:
        tags = args.tags.split()
        if args.tag not in tags:
            tags.append(args.tag)
        for tag in tags:
            filter_feature(args, tag)
    else:
        filter_feature(args, args.tag)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-filename", dest="input_name", help="Path to input file")
    parser.add_argument("-t", "--filtered-tag", dest="tag", help="The tag to be filtered from the file" )
    parser.add_argument("-tags", "--filtered-tags", dest="tags", help="A list of tags to be filtered from the file, use space as delimiter to separate different tags")
    args = parser.parse_args()
    
    if not args.input_name:
        print("Expected to have input xml file, but got none")
        sys.exit()
    if not (args.tag or args.tags):
        print("Expected to state tag(s) to be filtered, but got none")
        sys.exit()
    main(args)


            