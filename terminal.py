#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from autoDownload import AutoDownload
from rich import print as richPrint
import os


def main():
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument('Url', help = 'The URL of the file')
    argparser.add_argument('-f', '--filename', type = str, default="", help="The filename of the file")
    argparser.add_argument('-n', '--threadnum',type = int, default = 0, help = 'How many thread you want to download. 0 or lower means auto')
    argparser.add_argument('-m', '--max',type = int, default = 10, help = 'The max number of threads to download. It has to be greater than 0')
    argparser.add_argument('-r', '--retry', type = int, default = 5, help = "Max retry times for the first connection. If it's less than 0, it means infinity")
    argparser.add_argument('-tr', '--threadRetry', type = int, default = None, help = "Max retry times for the other thread. If it's less than 0, it means infinity")
    argparser.add_argument('-H', '--header', type = str, default = "{}", help = 'Header of the requests')
    argparser.add_argument('-w', '--wish', type = float, default = 10.0, help = 'time in seconds. It is our reference value for calculating the threadNum')
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
        if filename=="":
            raise ValueError("Can not get the name of the file by URL. Please set it by '-f' or '--filename'")
        retsult=AutoDownload(
            url = args.Url,
            file = filename,
            maxRetry=args.retry,
            threadNum=args.threadnum,
            maxThreadNum=args.max,
            maxThreadRetry=args.threadRetry,
            header=headers,
            desiredCompletionTime=args.wish
        ).start()
        if retsult:
            richPrint("[green]Successfully downloaded the file.[/green]")
            richPrint(f"The file was saved at [yellow]{os.path.abspath(filename)}[/yellow]")
        else:
            richPrint("[red]Failed to download[/red]")
    except:
        richPrint("[red]Failed to download[/red]")
        raise