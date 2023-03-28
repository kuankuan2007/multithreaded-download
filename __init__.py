import requests
import logging
import threading
import rich.progress
import os
from typing import *
import tempfile
import random
import time
class ConnectError(Exception):
    def __init__(self,url:str) -> None:
        super().__init__(self)
        self.url=url
    def __str__(self) -> str:
        return "Can not connect to %s" % self.url
class ZeroSizeError(Exception):
    def __init__(self,url:str) -> None:
        super().__init__(self)
        self.url=url
    def __str__(self) -> str:
        return "Can not get the  size of %s" % self.url
class _Part:
    def __init__(self,start_,to,fileName:str=None,stream:None|requests.Response=None) -> None:
        self.start=start_
        self.to=to
        self.fileName=fileName
        self.speed=0
        self.retryTime=0
        self.statue="init"
        self.stream=stream
        self.progress=None
        self.now=0
        self.histoyTime=0
        self.historyNum=0
        self.speeds=""
        self.statueNum=0
    def split(self,position):
        new=_Part(position,self.to)
        self.to=position
        print(new.__dict__)
        return new
class AutoDownload:
    def __init__(self,url:str,file:str,chunkSize:int=1024,maxRetry:int=5,continueDownloadTest:bool=False,startSize:int=0,openType:str="wb",
                 error:bool=True,log:bool=True,showProgressBar:bool=True,threaded:bool=False,threadNum=0,maxThreadNum=10,desiredCompletionTime=30,
                 callbackFunction:None|Callable=None,deamon=False,header:dict={})->None:
        """
        Download file from url to file
        :param url: url to download
        :param file: file name
        :param chunkSize: chunk size
        :param maxRetry: max retry times
        :param continueDownloadTest: Whether to detect power interruption
        :param startSize: where to start downloading. If continueDownloadTest is True, it will be covered
        :param openType: file open type. If continueDownloadTest is True, it will be covered
        :param error: whether to raise error
        :param log: whether to print log
        :param showProgressBar: whether to show progress bar
        :param threaded: whether to use thread for main thread
        :param callbackFunction: callback function
        :param threadNum: num of thread. If threadNum < 1, we'll automatically calculates the threadNum based on the total size and the speed of the first thread
        :param maxThreadNum: max num of thread. It will be overlooked if threadNum >= 1
        :param desiredCompletionTime: time in seconds. It is our reference value for calculating the threadNum
        :param deamon: whether to run in deamon mode
        :param header: request header
        """
        self.url = url
        self.file = file
        self.chunkSize = chunkSize
        self.maxRetry = maxRetry
        self.continueDownloadTest = continueDownloadTest
        self.startSize = startSize
        self.error = error
        self.log = log
        self.showProgressBar = showProgressBar
        self.threaded = threaded
        self.callbackFunction = callbackFunction
        self.deamon=deamon
        self.header=header
        self.threadNum=threadNum
        self.maxThreadNum=maxThreadNum
        self.desiredCompletionTime=desiredCompletionTime
        self._threadPool:List[threading.Thread] = []
        self._partition:List[_Part] =[]
        self._waitList:List[int]=[]
        self.logger=logging.getLogger("Download")
        self.openType=openType
        if self.showProgressBar:
            self.progress=rich.progress.Progress(
                rich.progress.TextColumn("[progress.description]{task.description}"),
                rich.progress.BarColumn(),
                rich.progress.TextColumn("[blue]{task.fields[now]}[/blue]"),
                rich.progress.TextColumn("[blue]/[/blue]"),
                rich.progress.TextColumn("[blue]{task.fields[size]}[/blue]"),
                rich.progress.TaskProgressColumn(),
                rich.progress.TextColumn("[blue]ETA:[/blue]"),
                rich.progress.TimeRemainingColumn(),
                rich.progress.TextColumn("[red]{task.fields[speed]}[/red]"),
                rich.progress.TextColumn("{task.fields[statue]}"),
                transient=True
            )
        self.tempFileDir=os.path.join(tempfile.gettempdir(),self.url.split("/")[-1]+str(random.random()))
    def _logShower(self,msg,level=logging.INFO):
        if not self.log:
            return
        self.logger.log(level=level,msg=msg)
    def _errorShower(self,err:Exception):
        self._logShower("%s:%s"%(err.__class__.__name__,str(err)),logging.ERROR)
        if self.error:
            raise err
    def start(self)->bool|None:
        if self.continueDownloadTest:
            if os.path.isfile(self.file) and os.access(self.file,os.W_OK):#文件是否存在 and 是否可读
                self.startSize=os.path.getsize(self.file)
                self.openType="ab"
            else:
                self._errorShower(FileNotFoundError("Can not open file '%s' for download."%(self.file)))
                return False
        os.makedirs(self.tempFileDir)
        if self.threaded:threading.Thread(target=self._wait,daemon=self.deamon,name="Download controller")
        else:return self._wait()
    def changeUnit(self,num:int|float):
        units=["B","KB","MB","GB","TB"]
        for i in range(len(units)):
            if num/(1024**i)<1024:
                return "%.2f%s"%((num/(1024**i)),units[i])
    def _progressUpgrade(self,part:_Part,length:int):
        t=int(time.time())
        if t!=part.histoyTime:
            part.speed=part.historyNum
            part.speeds=self.changeUnit(part.speed)+"/s"
            part.histoyTime=t
            part.historyNum=length
        part.historyNum+=length
        part.now+=length
    def _updateProgressBar(self):
        if not self.showProgressBar:
            return
        colors=["red","blue","green","gray","purple"]
        statuesColor=["blue","yellow","green","red"]
        with self.progress:
            total=self.progress.add_task("[yellow]Total",total=self.fileSize,speed="",size="",now="",statue="")
            while True:
                time.sleep(0.1)
                sums=0
                speedsum=0
                num=0
                for i in self._partition:
                    num+=1
                    sums+=i.now
                    speedsum+=i.speed
                    if i.progress!=None:
                        self.progress.update(i.progress,completed =i.now,total=i.to-i.start,speed=i.speeds,size=self.changeUnit(i.to-i.start),now=self.changeUnit(i.now),statue=f"[{statuesColor[i.statueNum]}]{i.statue}[/{statuesColor[i.statueNum]}]")
                    else:
                        i.progress=self.progress.add_task("[%s]Thread%d"%(random.choice(colors),num),total=i.to-i.start,speed="",size="",now="",statue="init")
                self.progress.update(total,completed =sums,total=self.fileSize,speed=self.changeUnit(speedsum)+"/s",size=self.changeUnit(self.fileSize),now=self.changeUnit(sums))
    def _splitThread(self):
        if self.threadNum<1:
            while self._partition[0].speed==0:
                time.sleep(1)
            time.sleep(1)
            self.threadNum=min(self.maxThreadNum,(self.fileSize)//(self._partition[0].speed*self.desiredCompletionTime))
        for i in range(1,self.threadNum):
            self._partition.append(self._partition[-1].split(self._partition[-1].start+self._partition[-1].now+(self.fileSize-self._partition[0].now)//(self.threadNum)))
            self._partition[-1].fileName=os.path.join(self.tempFileDir,f"{len(self._partition)}.tmp")
            self._threadPool.append(threading.Thread(target=self._download,daemon=self.deamon,args=[len(self._partition)-1]))
            self._waitList.append(i)
            self._threadPool[-1].start()
    def _wait(self)->bool:
        retsult=self._controller()
        if retsult:
            self._logShower("Successfully!")
        else:
            self._logShower("Fail!",level=logging.ERROR)
        if self.callbackFunction!=None:
            self.callbackFunction(retsult)
    def _controller(self)->bool:
        firstHeader=self.header.copy()
        firstHeader["Range"]="bytes=%d-"%(self.startSize)
        for i in range(self.maxRetry):
            try:
                retsult=requests.get(self.url,headers=firstHeader,stream=True)
                if retsult.status_code//100 not in [2,3]:
                    raise ConnectError(self.url)
                if 'content-length' not in retsult.headers:
                    self._logShower("Can not get the length of the file. try to download normally")
                    with open(self.file,self.openType) as f:
                        f.write(retsult.content)
                    return True
                self.fileSize=int(retsult.headers['content-length'])
                if not self.fileSize:
                    raise ZeroDivisionError(self.url)
                self._partition.append(_Part(self.startSize,self.startSize+int(self.fileSize),os.path.join(self.tempFileDir,"1.tmp"),retsult))
                self._threadPool.append(threading.Thread(target=self._download,daemon=self.deamon,args=[0]))
                self._waitList.append(0)
                self._threadPool[0].start()
                break
                
            except BaseException as err:
                if i==self.maxRetry-1:
                    self._errorShower(err)
                    return False
        threading.Thread(target=self._splitThread,daemon=True).start()
        threading.Thread(target=self._updateProgressBar,daemon=True).start()
        while True:
            time.sleep(0.5)
            if not self._waitList:
                self._logShower("All download finished. Start splicing",level=logging.DEBUG)
                splicing=self.progress.add_task("[yellow]splicing",total=self.fileSize,speed="",size="",now="",statue="")
                with open(self.file,self.openType) as wf:
                    for i in self._partition:
                        with open(i.fileName,"rb") as f:
                            data=f.read()
                            data=data[:i.to-i.start]
                            wf.write(data)
                            self.progress.update(splicing,advance=len(data))
                        os.remove(i.fileName)
                return True

                            
    def _download(self,partNum:int)->None:
        part=self._partition[partNum]
        header=self.header.copy()
        header["Range"]="bytes=%d-"%(part.start)
        part.statue="connecting"
        part.statueNum=1
        retryNum=0
        while True:
            try:
                if part.stream==None:
                    part.stream=requests.get(self.url,headers=header,stream=True)
                if part.stream.status_code//100 not in [2,3]:
                    raise ConnectError(self.url)
                with open(part.fileName,"wb") as f:
                    
                    if retryNum:
                        part.statue=f"R:{retryNum} downloading"
                    else:
                        part.statue="downloading"
                    self._logShower(f"Part {partNum} start downloading",level=logging.DEBUG)
                    part.statueNum=2
                    for data in part.stream.iter_content(chunk_size=self.chunkSize):
                        if part.start+part.now>part.to:
                            part.statue="finished"
                            part.statueNum=3
                            self._waitList.remove(partNum)
                            part.now=part.to-part.start
                            part.speed=0
                            part.speeds="--"
                            self._logShower(f"Part {partNum} is finished",level=logging.DEBUG)
                            return
                        f.write(data)
                        self._progressUpgrade(part,len(data))
                    part.statue="finished"
                    part.statueNum=3
                    self._waitList.remove(partNum)
                    part.now=part.to-part.start
                    return
            except BaseException as err:
                retryNum+=1
                self.statue=f"retry {retryNum}"
                part.statueNum=1
                self._logShower("Part %d %s:%s"%(partNum,err.__class__.__name__,str(err)),level=logging.WARNING)