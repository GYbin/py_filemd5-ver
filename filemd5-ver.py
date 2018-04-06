#!/usr/bin/python
#-*- coding: UTF-8 -*-

import os
import sys
import re
import sqlite3
import imagehash  #计算图片hash
from hashlib import md5
from PIL import Image  #计算图片hash
Image.LOAD_TRUNCATED_IMAGES = True
sys.path.append('../ybpy_tool/')
import ybpy_tool
reload(sys)  #出现编码错误打开
sys.setdefaultencoding('utf8')  #出现编码错误打开
run_ecc = 5
db_url = '../../data/py_filemd5-ver/filemd5-ver.db'
ybpy_tool.log_config( log_name ='py_filemd5-ver.log',log_dir = '../../log/') #日志记录位置

def add_database(): #建立数据库
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
    sqlite_cur.execute(''' CREATE TABLE photo_md5
        (id INTEGER PRIMARY KEY NOT NULL,
         file_list text,
         file_name text,
         file_md5 text,
         run_ecc int)''')
    sqlite_cur.execute(''' CREATE TABLE repeat_photo
        (id INTEGER PRIMARY KEY NOT NULL,
         file_list text,
         file_name text,
         file_md5 text,
         run_ecc int)''')


    sqlite_conn.commit()

def md5cal_big(file): #计算大文件MD5
    m = md5()
    f = open(file,'rb')
    buffer = 8192 # why is 8192 | 8192 is fast than 2048
    while 1:
        chunk = f.read(buffer)
        if not chunk : break
        m.update(chunk)
    f.close()
    return m.hexdigest()

def md5cal(file_tmp): #计算小文件MD5
    statinfo = os.stat(file_tmp) #获取文件信息
    if int(statinfo.st_size)/(1024*1024) >= 100:
        print "File size >100 ,move to big file..."
        return  md5cal_big(file_tmp)
    m = md5()
    f = open(file_tmp,'rb')
    m.update(f.read())
    f.close()
    return m.hexdigest()

def photo_md5cal(file_path):
    hash = imagehash.phash(Image.open(file_path))
    return hash

def file_add(file_tmp,md5num,sql_tab): #文件MD5入库
    file_list = os.path.split(file_tmp) # 目录与文件名分离
    list_tmp = [(file_list[0],file_list[1],md5num)]
    sql_cmd = '''INSERT INTO %s(file_list,file_name,file_md5,run_ecc)\
        VALUES( ?,?,'%s',%d) '''% (sql_tab,md5num,run_ecc)
    try :
        sqlite_cur.execute(sql_cmd,(file_list[0].decode('utf8'),file_list[1].decode('utf8')))
    except :
        print file_list[0],'-----',file_list[1]
        print  sql_cmd,'-------',file_tmp
        print "错误"
#        ybpy_tool.logger.error('SQL命令行错误：%s,%s',sql_cmd,file_tmp)
        return 1
    sqlite_conn.commit()
    return 0

def md5compare(data_tmp,md5num): #判断s是否有重复文件
    sql_cmd = ''' SELECT id,file_md5,run_ecc from %s where file_md5 = '%s' ''' %(data_tmp,md5num)
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

def run_ecc_up(idnum): #更新ex效验码（效验码作用，避免重复扫描目录，认为文件重复）
    sql_cmd = ''' UPDATE  filelist_md5 SET run_ecc = %d WHERE id = %d ''' %(run_ecc,int(idnum))
    sql_num = sqlite_cur.execute(sql_cmd)
    return 0 

def file_scan(path,md5cal = None, md5compare = None ,file_add = None): #扫描重复文件，并计算MD5
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
            return_nmu = md5compare('filelist_md5',md5num)
            if  return_nmu == 2 :
                file_add(tmp,md5num,'filelist_md5')
                print '  添加文件:' + tmp
            elif return_nmu == 1 :
                file_add(tmp,md5num,'repeat_file')
                print "重复文件:" + tmp
            else:
                print "更新 ECC - 无操作"
    sqlite_conn.commit()
    return ret

def photo_scan(path,path_jpg):
    path = path + '%'
    path_jpg = '%.'+ path_jpg
    for sql_tmp in photo_cf('filelist_md5','file_list',path,'file_name',path_jpg):
        file_url_tmp = sql_tmp[1] + u'/'+ sql_tmp[2]
        md5num = photo_md5cal(file_url_tmp)
        return_nmu = md5compare('photo_md5',md5num)
        if  return_nmu == 2 :
            file_add(file_url_tmp,md5num,'photo_md5')
            print u'  添加照片:' + file_url_tmp
        elif return_nmu == 1 :
            file_add(file_url_tmp,md5num,'repeat_photo')
            print "重复照片:" + file_url_tmp
        else:
            print "更新 ECC- 无操作 "

def dis_cf( table_name,list_name,file_list = '%'): #查找数据库，显示数据中文件
    sql_cmd = ''' SELECT id,file_list,file_name,file_md5 from %s where %s LIKE  '%s' ''' %(table_name,list_name,file_list)
    #print sql_cmd
    sql_num = sqlite_cur.execute(sql_cmd)
    tmp =  sql_num.fetchall()
    return tmp

def photo_cf( table_name,list_name1,file_list1,list_name2,file_list2):
    sql_cmd = ''' SELECT id,file_list,file_name,file_md5 from %s where %s LIKE  '%s' AND %s LIKE  '%s' ''' %(table_name,list_name1,file_list1,list_name2,file_list2)
    sql_num = sqlite_cur.execute(sql_cmd)
    tmp =  sql_num.fetchall()
    return tmp

def del_sqljl( table_name,list_name,file_list= '%'): #删除sql记录
    sql_cmd = ''' DELETE FROM %s WHERE %s LIKE '%s'  ''' %(table_name,list_name,file_list)
    sql_num = sqlite_cur.execute(sql_cmd)
    tmp = sql_num.fetchall()
    return tmp

def del_filecf(cffile_name): #删除文件
    try:
    #print " 删除成功：%s" %cffile_name
        os.remove(cffile_name)
        print '删除成功:'
        print cffile_name
    except :
        if os.path.isdir(cffile_name):
            ybpy_tool.logger.error('路径是文件夹，非文件。')
        else:
            ybpy_tool.logger.error('删除错误，文件不存在或正在使用:')
            print cffile_name
        return 1
    return 0

def else_in_6():#显示数据库重复文件
    while 1:
        print '''        1，重复文件夹路径
        2，重复文件夹名称
        3，根据MD5查询文件
        4，显示照片数据库'
        5，显示照片重复数据库
        6，根据MD5显示数据库照片
        9，退出'''
        url_in = raw_input("请输入：")
        if url_in == '9' :
            return 0
        elif url_in == '1':
            url_in = raw_input("请输入路径：")
            url_in = url_in + '%'
            for sql_tmp in dis_cf('repeat_file','file_list',url_in):
                print sql_tmp[0],sql_tmp[1] + u'/'+ sql_tmp[2],sql_tmp[3]
        elif url_in == '2':
            url_in = raw_input("请输入名称：")
            url_in = '%' + url_in + '%'
            for sql_tmp in dis_cf('repeat_file','file_list',url_in):
                print sql_tmp[0],sql_tmp[1] + u'/'+ sql_tmp[2],sql_tmp[3]
        elif url_in == '3':
            url_in = raw_input("请输入文件MD5：")
            for sql_tmp in dis_cf('filelist_md5','file_md5',url_in):
                print sql_tmp[0],sql_tmp[1] + u'/'+ sql_tmp[2],sql_tmp[3]
        elif url_in == '4':
            for sql_tmp in dis_cf('photo_md5','file_list','%'):
                print sql_tmp[0],sql_tmp[1] + u'/'+ sql_tmp[2],sql_tmp[3]
        elif url_in == '5':
            for sql_tmp in dis_cf('repeat_photo','file_list','%'):
                print sql_tmp[0],sql_tmp[1] + u'/'+ sql_tmp[2],sql_tmp[3]
        elif url_in == '6':
            url_in = raw_input("请输入文件MD5：")
            for sql_tmp in dis_cf('repeat_photo','file_md5',url_in):
                print sql_tmp[0],sql_tmp[1] + u'/'+ sql_tmp[2],sql_tmp[3]

def slse_in7(): #各种删除操作
    while 1:
        print '''        1,删除指定重复文件
        2,删除重复数据库中记录
        7,清理重复数据库无效记录
        8,显示数据库中全部重复文件
        9,退出'''
        url_in = raw_input('请输入：')
        if url_in == '9':
            return 0
        elif url_in == '1':
           url_in = raw_input('请输入路径:')
           url_in = url_in + '%'
           for sql_tmp in dis_cf('repeat_file','file_list',url_in):
               file_listtmp = sql_tmp[1] + u'/'+ sql_tmp[2]
               del_filecf(file_listtmp)
        elif url_in == '2':
           url_in = raw_input('请输入路径:')
           for sql_tmp in dis_cf('repeat_file','file_list',url_in):
               del_sqljl( 'repeat_file','id',sql_tmp[0])  
               print '删除SQL记录成功'
               print sql_tmp[1] + u'/'+ sql_tmp[2] 
           sqlite_conn.commit()
        elif url_in =='7':
           for sql_tmp in dis_cf('repeat_file','file_list'):
               if os.path.exists(sql_tmp[1] + u'/'+ sql_tmp[2]):
                   print "文件存在"
                   print sql_tmp[1] + u'/'+ sql_tmp[2]
               else:
                   del_sqljl( 'repeat_file','id',sql_tmp[0])
                   print '删除SQL记录成功'
           sqlite_conn.commit()
        elif url_in == '8':
             for sql_tmp in dis_cf('repeat_file','file_list','%'):
                 print sql_tmp[0],sql_tmp[1] + u'/'+ sql_tmp[2],sql_tmp[3]
 
def slse_in7():
    while 1:
        print '''        1,清除MD5文件数据库
        2,清除重复文件数据库
        3,清除图片数据库
        4,清除重复图片数据库
        9,退出'''
        url_in = raw_input('请输入：')
        if url_in == '9':
            return 0
        elif url_in == '1':
            for sql_tmp in dis_cf('filelist_md5','file_list','%'):
                del_sqljl( 'filelist_md5','id',sql_tmp[0])
                print sql_tmp[1] + u'/'+ sql_tmp[2] 
            print '删除SQL记录成功'
            sqlite_conn.commit()
        elif url_in == '2':
            for sql_tmp in dis_cf('repeat_file','file_list','%'):
                del_sqljl( 'repeat_file','id',sql_tmp[0])
                print sql_tmp[1] + u'/'+ sql_tmp[2]
            print '删除SQL记录成功'
            sqlite_conn.commit()
        elif url_in == '3':
            for sql_tmp in dis_cf('photo_md5','file_list','%'):
                del_sqljl( 'photo_md5','id',sql_tmp[0])
                print sql_tmp[1] + u'/'+ sql_tmp[2]
            print '删除SQL记录成功'
            sqlite_conn.commit()
        elif url_in == '4':
            for sql_tmp in dis_cf('repeat_photo','file_list','%'):
                del_sqljl( 'repeat_photo','id',sql_tmp[0])
                print sql_tmp[1] + u'/'+ sql_tmp[2]
            print '删除SQL记录成功'
            sqlite_conn.commit()


print """

"""
file_list = os.path.split(db_url)
if not os.path.exists(file_list[0]):
    print "数据库路径不存在..."
    os.makedirs(file_list[0])
    print "创建文件夹:" + file_list[0]
if not os.path.exists(db_url):
    print "创建数据库..."
    sqlite_conn = sqlite3.connect(db_url)
    print '初始化数据库...'
    sqlite_cur = sqlite_conn.cursor()
    add_database()
else:
    sqlite_conn = sqlite3.connect(db_url)
    sqlite_cur = sqlite_conn.cursor()

print '''数据库路径：''' + os.path.abspath(db_url)
print " "
print "程序路径：" + os.getcwd() 
print " "
print ''' 1：扫描重复文件
 2：扫描图片重复（根据选项1建立的数据库）
 6：显示数据库文件
 7：各种删除操作
 8：清除指定数据库
 9：退出
  '''
else_in = raw_input("请输入数字：")
if '9' == else_in:
    print "程序退出"
elif '1' == else_in: #扫描重复文件
    url_in = raw_input("请输入目录：")
    file_scan(url_in, md5cal,md5compare,file_add)
elif '2' == else_in:
    url_in = raw_input('请输入目录：')
    photo_scan(url_in,'jpg')


elif '6' == else_in:
    else_in_6() #显示数据库文件
elif '7' == else_in:
    slse_in7() #各种删除操作
elif '8' == else_in:
    print '清理数据库完成...'

sqlite_conn.close()
