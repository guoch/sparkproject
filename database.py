# -*- coding: utf-8 -*-
import sys 
reload(sys)
sys.setdefaultencoding('utf-8')
'''
Noted by Guo Chenghao
该模块用于写入抓取到的内容到Hbase,使用Happybase模块
'''
import happybase
conn = happybase.Connection('10.141.211.91')
table=conn.table('news')

def createtable():
	#conn.create_table('newstest',{'f:time':dict(),'f:title':dict(),'f:content':dict()})
	conn.create_table('newstest4',{'news': dict(max_versions=3)})

def storehbase(url,time,title,content):
	table.put(url,{'f:time':time,'f:title':title,'f:content':content})
	##hbase OK
	# conn.create_table('project',{'f':dict()})
	# for key,data in table.scan():
		# print key,data
def getrow(rowid):
	table=conn.table('test3')
	row=table.row(rowid)
	return row
	

# table.put('row-key', {'family:qual1': 'value1',
#                       'family:qual2': 'value2'})

# row = table.row('row-key')
# print row['family:qual1']  # prints 'value1'

# for key, data in table.rows(['row-key-1', 'row-key-2']):
# 	print key, data  # prints row key and data for each row

# for key, data in table.scan(row_prefix='row'):
# 	print key, data  # prints 'value1' and 'value2'

# row = table.delete('row-key')