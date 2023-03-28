#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from autoDownload import AutoDownload

def main():
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument('Url', help = 'The URL of the file')
    argparser.add_argument('-f', '--filename', type = str, default="", help="The filename of the file")
    argparser.add_argument('-n', '--threadnum',type = int, choices = range(0,20), default = 0, help = 'How many thread you want to download. 0 means auto')
    argparser.add_argument('-m', '--max',type = int, choices = range(1,20), default = 10, help = 'The max number of threads to download')
    argparser.add_argument('-r', '--retry', type = int, choices = range(0,100), default = 5, help = "Max retry times")
    argparser.add_argument('-H', '--header', type = str, default = "{}", help = 'Header of the requests')
    argparser.add_argument('-w', '--wish', type = int, default = 10, help = 'time in seconds. It is our reference value for calculating the threadNum')
    args = argparser.parse_args()
    
    try:
        headers = eval(args.header)
        if type(headers)!=dict:
            raise
    except:
        raise ValueError("Header should be a dict")
    if args.filename=="":
        filename = args.Url.split('/')[-1]
        filename = filename.split("?")[0]
    else:
        filename=args.filename
    try:
        retsult=AutoDownload(
            url = args.Url,
            file = filename,
            maxRetry=args.retry,
            threadNum=args.threadnum,
            maxThreadNum=args.max,
            header=headers,
            desiredCompletionTime=args.wish
        ).start()
        if retsult:
            print("Successfully")
        else:
            print("Failed to download")
    except:
        raise