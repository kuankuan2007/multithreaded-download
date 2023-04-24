import requests
import logging
import threading
import rich.progress
import rich.text
import os
from typing import Callable,Any,List,Union
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
class MyTimeRemainingColumn(rich.progress.TimeRemainingColumn):
    def render(self, task: rich.progress.Task) -> rich.text.Text:
        """Show time remaining."""
        if self.elapsed_when_finished and task.finished:
            task_time = task.finished_time
            style = "progress.elapsed"
        else:
            task_time = task.time_remaining
            style = "progress.remaining"

        if task.total is None:
            return rich.text.Text("unknown", style=style)

        if task_time is None:
            return rich.text.Text("--:--" if self.compact else "-:--:--", style=style)

        # Based on https://github.com/tqdm/tqdm/blob/master/tqdm/std.py
        minutes, seconds = divmod(int(task_time), 60)
        hours, minutes = divmod(minutes, 60)

        if self.compact and not hours:
            formatted = f"{minutes:02d}:{seconds:02d}"
        else:
            formatted = f"{hours:d}:{minutes:02d}:{seconds:02d}"

        return rich.text.Text(formatted, style=style)
class _Part:
    """
    A download task. It mean a part of the file we are downloading.
    It include the range of this task, the response, the statue, etc.
    """
    def __init__(self,start_:int,to:int,num:int=0,fileName:str="",stream:Union[None,requests.Response]=None) -> None:
        """
        New a Part object
        :param start_: the start position of the part
        :param to: the end position of the part
        :param num: the num of the part
        :param fileName: the tempfile name of the part
        :param stream: the response of the part
        """
        self.num=num
        self.start=start_
        self.to=to
        self.fileName=fileName
        self.speed=0
        self.retryTime=0
        self.statue="init"
        self.stream=stream
        self.progress:Union[None,rich.progress.TaskID]=None
        self.now:int=0
        self.histoyTime:int=0
        self.historyNum:int=0
        self.speeds:str=""
        self.statueNum:int=0
        self.startTime:float=1.0
    def split(self,position:int):
        """
        Split the part into two parts. If the position is out of range, it will return empty _Part object after the self.to
        :param position: the position of the part we want to split
        :return: a new _Part object
        """
        if position>=self.to or position<=self.start:
            return _Part(self.to,self.to)
        new=_Part(position,self.to)
        self.to=position
        return new
    def __lt__(self,other):
        if self.start!=other.start:
            return  self.start<other.start
        return self.to<other.to
class AutoDownload:
    def __init__(self,url:str,file:str,chunkSize:int=1024,maxRetry:int=5,maxThreadRetry:int=-1,timeout:Union[int,None]=30,continueDownloadTest:bool=False,startSize:int=0,openType:str="wb",
                 error:bool=True,log:bool=True,showProgressBar:bool=True,transient:bool=False,
                 threaded:bool=False,threadNum:int=0,maxThreadNum:int=10,desiredCompletionTime:int=30,
                 callbackFunction:Union[None,Callable[[bool], Any]]=None,deamon:bool=False,header:dict={})->None:
        """
        Download file from url to file
        :param url: url to download
        :param file: file name
        :param chunkSize: chunk size
        :param maxRetry: max retry times for the first connection. If it's less than 0, it means infinity
        :param maxThreadRetry: max retry times for the other connection. If it's less than 0, it means infinity
        :param continueDownloadTest: Whether to detect power interruption.
        :param startSize: where to start downloading. If continueDownloadTest is True, it will be covered
        :param openType: file open type. If continueDownloadTest is True, it will be covered
        :param error: whether to raise error
        :param log: whether to print log
        :param showProgressBar: whether to show progress bar
        :param transient: whether to keep progress after download
        :param threaded: whether to use thread for main thread
        :param callbackFunction: callback function
        :param threadNum: num of thread. If threadNum < 1, we'll automatically calculates the threadNum based on the total size and the speed of the first thread
        :param maxThreadNum: max num of thread. It will be overlooked if threadNum >= 1
        :param desiredCompletionTime: time in seconds. It is our reference value for calculating the threadNum
        :param deamon: whether to run in deamon mode.
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
        self.timeout=timeout
        self.maxThreadRetry=maxThreadRetry
        self.fileSize:int=-1
        self.progress=rich.progress.Progress(
            rich.progress.TextColumn("[progress.description]{task.description}"),
            rich.progress.BarColumn(),
            rich.progress.TextColumn("[blue]{task.fields[now]}[/blue]"),
            rich.progress.TextColumn("[blue]/[/blue]"),
            rich.progress.TextColumn("[blue]{task.fields[size]}[/blue]"),
            rich.progress.TaskProgressColumn(),
            rich.progress.TextColumn("[blue]ETA:[/blue]"),
            MyTimeRemainingColumn(),
            rich.progress.TextColumn("[red]{task.fields[speed]}[/red]"),
            rich.progress.TextColumn("{task.fields[statue]}"),
            transient=transient,
            disable=not showProgressBar
        )
        self.tempFileDir=os.path.join(tempfile.gettempdir(),self.url.split("/")[-1].split("?")[0]+str(random.random()))
        self.now=0
        self.speed=0
        self.speeds=""
        self.statue="init"
        self.historyTime=1
        self.historyNum=0
        self.fail=False
    def _logShower(self,msg:str,level=logging.INFO):
        """
        Show log message through with self.logger. And if self.log is False, it will do nothing.
        :param msg: the message
        :param level: the level of the message
        """
        if not self.log:
            return
        self.logger.log(level=level,msg=msg)
    def _errorShower(self,err:BaseException):
        """
        Show error message and raise it.
        :param err: the error
        """
        self._logShower("%s:%s"%(err.__class__.__name__,str(err)),logging.ERROR)
        self.fail=True
        if self.error:
            raise err
    def start(self)->Union[bool,None]:
        """
        Start the download.
        :return: True if success, False if fail, None if self.threaded is True.
        """
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
    def changeUnit(self,num:Union[int,float])->str:
        """
        Change the unit of the data size.
        :param num: the size of the data
        :return: the formatted string 
        """
        units=["B","KB","MB","GB","TB"]
        if num<0:
            return "unknown"
        for i in range(len(units)):
            if num/(1024**i)<1024:
                return "%.2f%s"%((num/(1024**i)),units[i])
        return ""
    def _progressUpgrade(self,part:_Part,length:int):
        """
        Updata the task.
        :param part: the part to be updated
        :param length: the length of the new data
        """
        t=int(time.time())
        if t!=part.histoyTime:
            part.speed=part.historyNum
            part.speeds=self.changeUnit(part.speed)+"/s"
            part.histoyTime=t
            part.historyNum=length
        if t!=self.historyTime and part.num==0:
            self.speed=self.historyNum
            self.speeds=self.changeUnit(self.speed)+"/s"
            self.historyTime=t
            self.historyNum=length
        self.historyNum+=length
        self.now+=length
        part.historyNum+=length
        part.now+=length
    def _updateProgressBar(self):
        """
        Control ProgressBar
        """
        colors=["red","blue","green","gray","purple"]
        statuesColor=["blue","yellow","green","red"]
        with self.progress:
            self.total=self.progress.add_task("[yellow]Total",total=self.fileSize,start=False,speed="",size="",now="",statue="")
            while True:
                time.sleep(0.1)
                num=0
                for i in self._partition:
                    num+=1
                    if i.progress!=None:
                        self.progress.update(i.progress,completed =i.now,total=i.to-i.start,speed=i.speeds,size=self.changeUnit(i.to-i.start),now=self.changeUnit(i.now),statue=f"[{statuesColor[i.statueNum]}]{i.statue}[/{statuesColor[i.statueNum]}]")
                    else:
                        i.progress=self.progress.add_task("[%s]Thread%d"%(random.choice(colors),num),total=i.to-i.start,speed="",size="",now="",statue="init")
                if self.fileSize>=0:
                    self.progress.start_task(self.total)
                self.progress.update(self.total,completed =self.now,total=self.fileSize,speed=self.speeds,size=self.changeUnit(self.fileSize),now=self.changeUnit(self.now),statue=f"[yellow]{self.statue}[/yellow]")
    def _splitThread(self):
        """
        Split the first part into multiple threads
        """
        if self.threadNum<1:
            while self._partition[0].speed==0:
                time.sleep(1)
            time.sleep(1)
            if self.maxThreadNum<=0:
                self._logShower("The maxThreadNum is less than or equal to 0. We try to download it in single thread.",level=logging.WARNING)
                return
            self.threadNum=min(self.maxThreadNum,int((self.fileSize)//(self._partition[0].speed*self.desiredCompletionTime)))
        if self.threadNum>1:
            if self._partition[0].start+self._partition[0].now+(self.fileSize-self._partition[0].now)//(self.threadNum)>=self._partition[0].to:
                return
            self._partition.append(self._partition[0].split(self._partition[0].start+self._partition[0].now+(self.fileSize-self._partition[0].now)//(self.threadNum)))
            self._partition[-1].num=1
            elseSize=self._partition[-1].to-self._partition[-1].start
            for i in range(2,self.threadNum):
                self._partition.append(self._partition[-1].split(self._partition[-1].start+elseSize//(self.threadNum-1)))
                self._partition[-1].num=i
                
            for i in range(1,len(self._partition)):
                self._partition[i].fileName=os.path.join(self.tempFileDir,f"{i}.tmp")
                self._threadPool.append(threading.Thread(target=self._download,daemon=True,args=[i]))
                self._waitList.append(i)
                self._threadPool[-1].start()
    def _wait(self)->bool:
        retsult=self._controller()
        self.progress.stop()
        if retsult:
            self._logShower("Successfully!")
        else:
            self._logShower("Fail!",level=logging.ERROR)
        if self.callbackFunction!=None:
            self.callbackFunction(retsult)
        return retsult
    def _controller(self)->bool:
        firstHeader=self.header.copy()
        firstHeader["Range"]="bytes=%d-"%(self.startSize)
        threading.Thread(target=self._updateProgressBar,daemon=True).start()
        for i in range(self.maxRetry):
            try:
                self.statue="connecting"
                retsult=requests.get(self.url,headers=firstHeader,stream=True,timeout=self.timeout)
                if retsult.status_code//100 not in [2,3]:
                    raise ConnectError(self.url)
                if 'content-length' not in retsult.headers:
                    self._logShower("Can not get the length of the file. try to download normally",level=logging.WARNING)
                    with open(self.file,self.openType) as f:
                        self.statue="downloading"
                        for i in retsult.iter_content(chunk_size=self.chunkSize):
                            t=int(time.time())
                            if t!=self.historyTime:
                                self.speed=self.historyNum
                                self.speeds=self.changeUnit(self.speed)+"/s"
                                self.histoyTime=t
                                self.historyNum=len(i)
                            self.historyNum+=len(i)
                            self.now+=len(i)
                            f.write(i)
                    
                    try:
                        self.progress.update(self.total,speed=self.speeds,statue="[yellow]finished[/yellow]",now=self.changeUnit(self.now))
                    except:
                        pass
                    self.progress.refresh()
                    return True
                self.fileSize=int(retsult.headers['content-length'])
                if self.fileSize<=0:
                    raise ZeroDivisionError(self.url)
                self.statue="downloading"
                self._partition.append(_Part(self.startSize,self.startSize+int(self.fileSize),0,os.path.join(self.tempFileDir,"0.tmp"),retsult))
                self._threadPool.append(threading.Thread(target=self._download,daemon=True,args=[0]))
                self._waitList.append(0)
                self._threadPool[0].start()
                break
                
            except BaseException as err:
                if i==self.maxRetry-1:
                    self._errorShower(err)
                    return False
        threading.Thread(target=self._splitThread,daemon=True).start()
        try:
            while True:
                time.sleep(0.5)
                if self.fail:
                    return False
                if not self._waitList:
                    self.statue="finished"
                    self.progress.update(self.total,completed=self.fileSize)
                    self._logShower("All download finished. Start splicing",level=logging.DEBUG)
                    splicing=self.progress.add_task("[yellow]splicing",total=self.fileSize,speed="",size="",now="",statue="")
                    self._partition.sort()
                    with open(self.file,self.openType) as wf:
                        for i in self._partition:
                            now=0
                            with open(i.fileName,"rb") as f:
                                while True:
                                    data=f.read(self.chunkSize)
                                    if now+len(data)>=i.to-i.start:
                                        wf.write(data[:i.to-i.start-now])
                                        self.progress.update(splicing,advance=i.to-i.start-now)
                                        break
                                    if not len(data):
                                        self._errorShower(ValueError("The size of the part is not enough"))
                                    now+=len(data)
                                    wf.write(data)
                                    self.progress.update(splicing,advance=len(data))
                            try:
                                os.remove(i.fileName)
                            except:
                                pass
                    self.progress.update(splicing,completed=self.fileSize,statue="[green]finished[/green]")
                    self.progress.refresh()
                    return True
        except BaseException as err:
            self._errorShower(err)
            return False
    def _finished(self)->None:
        """
        When a download thread is finished. Find another part which is the slowest to help.
        """
        least={
            "num":None,
            "rest":0
        }
        for i in range(len(self._partition)):
            if self._partition[i].statue=="finished":
                continue
            if self._partition[i].speed==0:
                if self._partition[i].now==0:
                    continue
                speed=self._partition[i].now/(time.time()-self._partition[i].startTime)
            else:
                speed=self._partition[i].speed
            if (self._partition[i].to-self._partition[i].start-self._partition[i].now)/speed>least["rest"]:
                least["num"]=i
                least["rest"]=(self._partition[i].to-self._partition[i].start-self._partition[i].now)/speed
        if least["num"]==None:
            self._logShower("No part need help, pass",level=logging.DEBUG)
            return
        self._logShower(f"Part {least['num']} is the slowest one. Split it.",level=logging.DEBUG)
        if least["rest"]>self.desiredCompletionTime:
            new=self._partition[least["num"]].split((self._partition[least["num"]].to-self._partition[least["num"]].start-self._partition[least["num"]].now)//2+self._partition[least["num"]].start+self._partition[least["num"]].now)
            self._partition.append(new)
            self._threadPool.append(threading.Thread(target=self._download,args=[len(self._partition)-1]))
            self._partition[-1].fileName=os.path.join(self.tempFileDir,f"{len(self._partition)-1}.tmp")
            self._waitList.append(len(self._partition)-1)
            self._threadPool[-1].start()

    def _download(self,partNum:int)->None:
        """
        The download thread
        :param partNum: the partition number
        """
        part=self._partition[partNum]
        header=self.header.copy()
        header["Range"]="bytes=%d-"%(part.start)
        part.statue="connecting"
        part.statueNum=1
        retryNum=0
        while True:
            try:
                if part.stream==None:
                    part.stream=requests.get(self.url,headers=header,stream=True,timeout=self.timeout)
                if part.stream.status_code//100 not in [2,3]:
                    raise ConnectError(self.url)
                with open(part.fileName,"wb") as f:
                    
                    if retryNum:
                        part.statue=f"R:{retryNum} downloading"
                    else:
                        part.statue="downloading"
                    self._logShower(f"Part {partNum} start downloading",level=logging.DEBUG)
                    part.statueNum=2
                    part.startTime=time.time()
                    part.now=0
                    for data in part.stream.iter_content(chunk_size=self.chunkSize):
                        if part.start+part.now>part.to:
                            part.statue="finished"
                            part.statueNum=3
                            self._waitList.remove(partNum)
                            part.now=part.to-part.start
                            part.speed=0
                            part.speeds="--"
                            self._logShower(f"Part {partNum} is finished",level=logging.DEBUG)
                            self._finished()
                            return
                        f.write(data)
                        self._progressUpgrade(part,len(data))
                    part.statue="finished"
                    part.statueNum=3
                    self._waitList.remove(partNum)
                    part.now=part.to-part.start
                    part.speed=0
                    part.speeds="--"
                    self._logShower(f"Part {partNum} is finished",level=logging.DEBUG)
                    self._finished()
                    return

            except BaseException as err:
                if retryNum==self.maxThreadRetry:
                    self._errorShower(err)
                retryNum+=1
                part.statue=f"retry {retryNum}"
                part.statueNum=1
                self._logShower("Part %d %s:%s"%(partNum,err.__class__.__name__,str(err)),level=logging.WARNING)