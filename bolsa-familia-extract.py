# -*- coding: utf-8 -*-
#!/usr/bin/pyhton
import argparse, zipfile, os, io

parser = argparse.ArgumentParser(description='Receives a file (text-csv or zip) as input and output each line to be inserted in hadoop HDFS')
parser.add_argument('filename', metavar='-f', help='Input filepath')
parser.add_argument('--zip', dest='isZip', action='store_true', help='If the input file is a zip file, this option should be used')
args = parser.parse_args()

if args.isZip and zipfile.is_zipfile(args.filename):
    with zipfile.ZipFile(args.filename, 'r') as compressed_file:
        zip_contents = compressed_file.namelist()
        for f in zip_contents:
            print 'Extracting %s from %s' % (f, args.filename)
            compressed_file.extract(f)
        
        #extract all the files, so can close the zip
        compressed_file.close()
        
        #after file is used, can remove it, so the directory is clear after processing the zip contents
        for f in zip_contents:
            with io.open(f, encoding='iso-8859-1') as stream:
                print 'Reading file %s' % (f)
                for line in stream:
                    hadoop_input = line.split(';')
                stream.close()
            os.remove(f)
            
            