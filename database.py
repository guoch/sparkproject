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
##hbase OK
# conn.create_table('project',{'f':dict()})
# for key,data in table.scan():
	# print key,data

table = conn.table('testbase')
connection.create_table('newstest',{'time':dict(),'title':dict(),'content':dict()})

# table.put('row-key', {'family:qual1': 'value1',
#                       'family:qual2': 'value2'})

# row = table.row('row-key')
# print row['family:qual1']  # prints 'value1'

# for key, data in table.rows(['row-key-1', 'row-key-2']):
# 	print key, data  # prints row key and data for each row

# for key, data in table.scan(row_prefix='row'):
# 	print key, data  # prints 'value1' and 'value2'

# row = table.delete('row-key')