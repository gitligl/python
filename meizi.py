# -*- coding=utf-8 -*-
# 郑重申明，本爬虫主要用来学习，一切用于非法目的的操作均与本人无关
# 爬取妹子图网站的图片
import requests
from bs4 import BeautifulSoup
import random
import time
import os

url = 'https://www.mzitu.com/'
#url = 'https://www.mzitu.com/taiwan/'
#url = 'https://www.mzitu.com/29221'
#url = 'https://i.meizitu.net/2019/04/08b01.jpg'

# 根据妹子图网址，获取需要爬取的分类url
def Get_classify_url(url):
    urls = []
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'}
    headers['Referer'] = url
    res = requests.get(url,headers=headers)
    reg = res.text
    soup = BeautifulSoup(reg,'html.parser')
    want = soup.select('ul li a')[1:5]      # 由于"首页"，"街拍美女"，"每日更新"比较特殊，这里只爬取其余四个类型
    for i in want:
        urls.append(i['href'])
    return urls
    
# 根据分类网址，获取每个妹子的url
def Get_meizi_url(url):
    urls = []
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'}
    headers['Referer'] = url
    res = requests.get(url,headers=headers)
    reg = res.text
    soup = BeautifulSoup(reg,'html.parser')
    # 每个分类下都有若干页，先获取该分类下总页数
    want = int(soup.select('div.nav-links a')[-2].text)
    print('这个类别的美女总共有',want,'页')
    # 根据每页的url获取该页面下每个妹子的url
    for i in range(1,want+1):
        new_url = url+'page/'+str(i)
        #print('爬取页面:',new_url)
        headers['Referer'] = new_url
        rs = requests.get(new_url,headers=headers)
        rg = rs.text
        soup1 = BeautifulSoup(rg,'html.parser')
        want1 = soup1.select('ul#pins li span a')
        for i in want1:
            urls.append(i['href'])
    return urls
    
# 根据每个妹子的url，获取其所有图片的url
def Get_meizi_pic(url):
    urls = []
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'}
    headers['Referer'] = url
    res = requests.get(url,headers=headers)
    reg = res.text
    soup = BeautifulSoup(reg,'html.parser')
    want = int(soup.select('.pagenavi')[0].select('a')[-2].text)
    print('总共有',want,'张这个美女的图片')
    for i in range(1,want+1):
        new_url = url+'/'+str(i)
        headers['Referer'] = new_url
        rs = requests.get(new_url,headers=headers)
        rg = rs.text
        soup1 = BeautifulSoup(rg,'html.parser')
        path_name = soup1.select('div.currentpath')[0].text.split(' » ')[-1]
        pic_name = soup1.select('h2')[0].text+'.jpg'
        root_name = soup1.select('div.currentpath a')[0].text
        classify_name = soup1.select('div.currentpath a')[1].text
        root_dir = 'D:\\'+root_name
        classify_dir = root_dir+'\\'+classify_name
        pic_dir = classify_dir+'\\'+path_name
        pic_path = pic_dir+'\\'+pic_name
        pic_url = soup1.select('div.main-image p a img')[0]['src']
        # 若该目录不存在，则先把目录创建成功
        try:
            if not os.path.isdir(pic_dir):
                try:
                    if not os.path.isdir(classify_dir):
                        try:
                            if not os.path.isdir(root_dir):
                                os.mkdir(root_dir)
                        except FileNotFoundError:
                            os.mkdir(root_dir)
                        os.mkdir(classify_dir)
                except FileNotFoundError:
                    os.mkdir(classify_dir)
                os.mkdir(pic_dir)
        except FileNotFoundError:
            os.mkdir(pic_dir)
        print('正在下载图片>>'+pic_name)
        Download_pic(pic_url,pic_path)
    
# 根据具体某一张图片的url，下载该图片
def Download_pic(url,path='D:\pic.jpg'):
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'}
    headers['Referer'] = url
    content = requests.get(url,headers=headers).content
    with open (path,'wb') as f:
        f.write(content)
    
if __name__=="__main__":
    for u in Get_classify_url(url):
        for r in Get_meizi_url(u):
            Get_meizi_pic(r)
            time.sleep(random.random()) # 暂停0~1秒，时间区间：[0,1)，做个随机休眠，防止网站判断为爬虫
