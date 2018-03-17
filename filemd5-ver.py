#!/usr/bin/python
#-*- coding: UTF-8 -*-

import os
import sys
import re
import sqlite3
from hashlib import md5
sys.path.append('../ybpy_tool/')
import ybpy_tool
run_ecc = 1
db_url = '../../data/py_filemd5-ver/filemd5-ver.db'
ybpy_tool.log_config( log_name ='py_filemd5-ver.log',log_dir = '../../log/') #日志记录位置

def add_database():
    sqlite_cur.execute(''' CREATE TABLE filelist_md5
	(id INTEGER PRIMARY KEY NOT NULL ,
	 file_list text,
	 file_name text,
         file_md5 text,
         run_ecc  int )''')
    sqlite_cur.execute(''' CREATE TABLE repeat_file
	(id INTEGER PRIMARY KEY NOT NULL,
	 file_list text,
	 file_name text,
	 file_md5 text,
         run_ecc int)''')
    sqlite_conn.commit()

def md5cal_big(file):
    m = md5()
    f = open(file,'rb')
    buffer = 8192 # why is 8192 | 8192 is fast than 2048
    while 1:
        chunk = f.read(buffer)
        if not chunk : break
        m.update(chunk)
    f.close()
    return m.hexdigest()

def md5cal(file):
    statinfo = os.stat(file) #获取文件信息
    if int(statinfo.st_size)/(1024*1024) >= 100:
        print "File size >100 ,move to big file..."
        return  md5cal_big(file)
    m = md5()
    f = open(file,'rb')
    m.update(f.read())
    f.close()
    return m.hexdigest()

def file_add(file,md5num,sql_tab):
    file_list = os.path.split(file)
    sql_cmd = '''INSERT INTO %s(file_list,file_name,file_md5,run_ecc)\
        VALUES( '%s' ,'%s', '%s', %d) '''%(sql_tab,file_list[0],file_list[1],md5num,run_ecc)
#    print sql_cmd
    sqlite_cur.execute( sql_cmd )
    sqlite_conn.commit()
    return 0

def md5compare(md5num):
    sql_cmd = ''' SELECT id,file_md5,run_ecc from filelist_md5 where file_md5 = '%s' ''' % md5num
    sql_num = sqlite_cur.execute(sql_cmd)
    tmp =  sql_num.fetchall()
    if tmp:
        if run_ecc ==  int(tmp[0][2]):
           return 1
        else:
           run_ecc_up(tmp[0][0])
           return 0
    else:
        return 2

def run_ecc_up(idnum):
    sql_cmd = ''' UPDATE  filelist_md5 SET run_ecc = %d WHERE id = %d ''' %(run_ecc,int(idnum))
    sql_num = sqlite_cur.execute(sql_cmd)
    return 0 
def file_scan(path,md5cal = None, md5compare = None ,file_add = None):
    stack = []
    ret = []
    stack.append(path)
    while len(stack) > 0:
        tmp = stack.pop(len(stack) - 1)
        if (os.path.isdir(tmp)):
            #ret.append(tmp)
            for item in os.listdir(tmp):
                stack.append(os.path.join(tmp,item))
           # print tmp
        elif (os.path.isfile(tmp)):
            #ret.append(tmp)
            md5num = md5cal(tmp)
            return_nmu = md5compare(md5num)
            if  return_nmu == 2 :
                file_add(tmp,md5num,'filelist_md5')
                print '  添加文件:' + tmp
            elif return_nmu == 1 :
                file_add(tmp,md5num,'repeat_file')
                print "重复文件:" + tmp
            else:
                print "更新 ECC"
    return ret

#file_scan('/mnt/d/1/', md5cal,md5compare,file_add)

def dis_cf( file_list = '%'):
    sql_cmd = ''' SELECT id,file_list,file_name,file_md5 from repeat_file where file_list LIKE  '%s' ''' % file_list
    sql_num = sqlite_cur.execute(sql_cmd)
    tmp =  sql_num.fetchall()
    for row in tmp:
        #print "ID:"+ str(row[0]) + '目录:' + row[1] +'文件名'+ row[2]
        print row[0],row[1] + u'/'+ row[2],row[3]
print """

"""

sqlite_conn = sqlite3.connect(db_url)
sqlite_cur = sqlite_conn.cursor()
print '''打开数据库 ...... 成功
数据库路径：''' + os.path.abspath(db_url)
print " "
print "程序路径：" + os.getcwd() 
print " "
print ''' 1：扫描重复文件
 2：显示重复文件
 3：删除重复文件(保留一份)
 4：初始化数据库
 9：退出
  '''
else_in = raw_input("请输入数字：")
if '9' == else_in:
    print "程序退出"
elif '2' == else_in:
    print '1，输入文件夹路径'
    print '2，输入文件夹名称'
    url_in = raw_input("请输入：")
    if url_in == '1' :
        url_in = raw_input("请输入路径：")
        url_in = url_in + '%'
        dis_cf(url_in)
    else:
        url_in = raw_input("请输入名称：")
        url_in = '%' + url_in + '%'
        dis_cf(url_in)
elif '1' == else_in:
    url_in = raw_input("请输入目录：")
    file_scan(url_in, md5cal,md5compare,file_add)
elif '3' == else_in:
    print '删除'
elif '4' == else_in:
    add_database()
    print '数据库初始化完成...'

sqlite_conn.close()
