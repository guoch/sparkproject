import happybase


conn=happybase.Connection('ubuntu1')
#hbase OK
conn.create_table('project',{'f':dict()})