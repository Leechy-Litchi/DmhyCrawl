import requests
from bs4 import BeautifulSoup
import multiprocessing
from multiprocessing.managers import BaseManager
import threading
from zhconv import convert
global list
global k
k = []
list = {}
sem = threading.Semaphore(8)
class get_magnet(threading.Thread):
    def __init__(self,text,href,num):
        threading.Thread.__init__(self)
        self.text = text
        self.href = href
        self.num = num
    def run(self): 
        with sem:
            # print("第"+str(self.num+1)+"正在获取")              
            try:
                get_magnet_single(self.text,self.href)
            except:
                print("错误：")
                print(self.text)
                print(self.href[0])
                pass 
def get_magnet_single(text,href):
            response = requests.get(href[0])
            soup = BeautifulSoup(response.content, 'html.parser')   
            magnet = soup.find("a",class_="magnet")
            list[text].append(magnet["href"])

 
lock = threading.Lock()

class get_url(threading.Thread):
    def __init__(self,page):
        threading.Thread.__init__(self)
        self.page = page
    def run(self): 
        with sem:
            get_url_single(self.page)

def get_url_single(page):
        print("正在获取第"+str(page)+"页")
        url = "https://dmhy.org/topics/list/sort_id/31/page/"+str(page) 
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')   
            torrentlist = []
            index = 0
            for torrents in soup.find_all("span",class_='btl_1'):
                torrentlist.append(str(torrents.text))
            if torrentlist==[]:
                return
            for hrefs in soup.find_all("a"):
                if "view" in hrefs['href']:
                    if torrentlist[index]=='-' or torrentlist[index]>'0':
                        href = "https://dmhy.org"+hrefs['href']
                        text = hrefs.text.strip()
                        lock.acquire()
                        list[convert(text,'zh-cn')] = [href]
                        lock.release()
                        index += 1
        except:
            print("获取第"+str(page)+"页错误")
            lock.release()
            pass

def get_list(start=1,end=315):
    import time
    ts = []
    while(start<=end):
        ts.append(get_url(start))
        start += 1
    try:
        for t in ts:
            t.start()        
        for t in ts:
            t.join()        
    finally:
        ts = []
    print("发布信息获取完成")
    print("共"+str(len(list))+"条")
    duclipcate()
    print("去重结束")
    print("共"+str(len(list))+"条")
    print("开始获取链接")
    iter = list.items()
    num = 0
    for text,href in iter:
        ts.append(get_magnet(text,href,num))
        num += 1
    num = 0
    for t in ts:
        t.start()
        num += 1
    for t in ts:
        t.join()               
    write_xlsx()

def duclipcate():
        ts = []
        iter = list.keys()
        for i in iter:
            k.append(i)
        i = 0
        while(i<len(k)):
            ts.append(duclipcate_once(i))
            i += 1
        for t in ts:
            t.start()
        for t in ts:
            t.join()

class duclipcate_once(threading.Thread):
    def __init__(self,i):
        threading.Thread.__init__(self)
        self.i = i
    def run(self): 
        try:
            duclipcate_single(self.i)
        except:
            if lock.locked():
                lock.release()
def duclipcate_single(i):   
            if(k[i]==None):
                return
            j = i+1
            # print("进行第"+str(i+1)+"条")
            while(j<len(k)):
                if(k[j]==None):
                    j += 1
                    continue
                if(get_ratio(k[i],k[j])):
                    lock.acquire()
                    list.pop(k[j])
                    k[j] = None
                    lock.release()
                j += 1

def get_ratio(first,second)->bool:
    import Levenshtein
    import re
    regex = re.compile("\[|\]|【|】")
    x = regex.split(first)
    y = regex.split(second)
    index = 0
    if len(x)<=2:
        s1 = x[0] if len(x)==1 else x[0] if len(x[0])<len(x[1]) else x[1]
    else:
        index = 2
        try:
            while(x[index]==None or x[index]=='' or '组' in x[index] or '新番' in x[index]):
                index += 1
            s1 = x[index].strip()   
        except:
            s1 = first
    index = 0
    if len(y)<=2:
        s2 = y[0] if len(y)==1 else y[0] if len(y[0])<len(y[1]) else y[1]
    else:
        index = 2
        try:
            while(y[index]==None or y[index]=='' or '组' in y[index] or '新番' in y[index] ):
                index += 1
            s2 = y[index].strip()  
        except:
            s2 = second
    length = len(s1) if len(s1)<len(s2) else len(s2)
    ratio = Levenshtein.jaro(s1[0:length],s2[0:length])
    return True if ratio > 0.7 else False

def write_xlsx():
    import pandas as pd
    newlist = {"番剧名称":[],"发布页面":[],"下载链接":[]}
    for i in list:
        try:
            if i != None and i != '':
                newlist["番剧名称"].append(i)
                newlist["发布页面"].append(list[i][0])
                newlist["下载链接"].append(list[i][1])
        except:
            print("格式化错误")
            print(i)
    data = pd.DataFrame(newlist)
    sheetNames = data.keys()    
    try:
        reader = pd.read_excel('./out.xlsx')
        data_all = pd.concat([data,reader ], ignore_index=True)
        data_all = data_all.drop_duplicates(keep='first')
        data_all.to_excel('./out.xlsx')   
    except FileNotFoundError:
        writer = pd.ExcelWriter('./out.xlsx')        
        for sheetName in sheetNames:
            data.to_excel(writer, sheet_name=sheetName)
        writer.close()    
    print("表格已生成")

if __name__ == '__main__':
    get_list()
