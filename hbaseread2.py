# -*- coding: utf-8 -*-
from __future__ import print_function
import json
# from jieba import analyse
import jieba
from pyspark import SparkContext
import sys 
reload(sys)
sys.setdefaultencoding('utf-8')


"""
author:Kira(Chenghao Guo)


根据时间筛选top k
Create test data in HBase first:

hbase(main):016:0> create 'test', 'f1'
0 row(s) in 1.0430 seconds

hbase(main):017:0> put 'test', 'row1', 'f1:a', 'value1'
0 row(s) in 0.0130 seconds

hbase(main):018:0> put 'test', 'row1', 'f1:b', 'value2'
0 row(s) in 0.0030 seconds

hbase(main):019:0> put 'test', 'row2', 'f1', 'value3'
0 row(s) in 0.0050 seconds

hbase(main):020:0> put 'test', 'row3', 'f1', 'value4'
0 row(s) in 0.0110 seconds

hbase(main):021:0> scan 'test'
ROW                           COLUMN+CELL
 row1                         column=f1:a, timestamp=1401883411986, value=value1
 row1                         column=f1:b, timestamp=1401883415212, value=value2
 row2                         column=f1:, timestamp=1401883417858, value=value3
 row3                         column=f1:, timestamp=1401883420805, value=value4
4 row(s) in 0.0240 seconds
"""
def wordcut(v):
    try:
        x=eval("'%s'"%v['value'])
    except Exception,ex:
        x='invalid'
    seglist=jieba.cut(x)
    # seglist=analyse.extract_tags(x,10)
    myvalue='|'.join(seglist)
    return myvalue

# def content_analyse(v):
#     try:
#         x=eval("'%s'"%v['value'])
#     except Exception,ex:
#         x='invalid'
#     # seglist=jieba.cut(x)
#     seglist=analyse.extract_tags(x,10)
#     myvalue='|'.join(seglist)
#     return myvalue

def inverted(v):
    url=v[0]
    return ((word,url) for word in v[1].split('|'))

def ridoff(ids):
    news_ids=list(set(ids))
    # news_ids.sort(ids.index)
    return news_ids

def hbaseput(sc,host,table,args):  #单独插入性能比较差，并行插入
    '''
        ./bin/spark-submit --driver-class-path /path/to/example/jar \
        /path/to/examples/hbase_outputformat.py <args>
        Assumes you have created <table> with column family <family> in HBase
        running on <host> already
        '''
    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    sc.parallelize([args]).map(lambda x: (x[0], x)).saveAsNewAPIHadoopDataset(
        conf=conf,
        keyConverter=keyConv,
        valueConverter=valueConv)



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("""
        Usage: hbase_inputformat <host> <table>

        Run with example jar:
        ./bin/spark-submit --driver-class-path /path/to/example/jar \
        /path/to/examples/hbase_inputformat.py <host> <table> [<znode>]
        Assumes you have some data in HBase already, running on <host>, in <table>
          optionally, you can specify parent znode for your hbase cluster - <znode>
        """, file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    table = sys.argv[2]
    # outputdir=sys.argv[3]
    sc = SparkContext(appName="HBaseInputFormat")
    # sc.addJar('/home/scidb/spark-1.5.2/lib/spark-examples-1.5.2-hadoop2.6.0.jar')

    # Other options for configuring scan behavior are available. More information available at
    # https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/mapreduce/TableInputFormat.java
    conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
    if len(sys.argv) > 3:
        conf = {"hbase.zookeeper.quorum": host, "zookeeper.znode.parent": sys.argv[3],
                "hbase.mapreduce.inputtable": table}
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

    hbase_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat","org.apache.hadoop.hbase.io.ImmutableBytesWritable","org.apache.hadoop.hbase.client.Result",keyConverter=keyConv,valueConverter=valueConv,conf=conf)
    hbase_rdd = hbase_rdd.flatMapValues(lambda v: v.split("\n")).mapValues(json.loads)

    hbase_rdd_title=hbase_rdd.filter(lambda keyValue: keyValue[1]['qualifier']=='title' and keyValue[1]['value']!=None)
    hbase_rdd_title=hbase_rdd_title.mapValues(wordcut)  #分析title中所有的关键词，title的权重更加重一些

    # hbase_rdd_content=hbase_rdd.filter(lambda keyValue: keyValue[1]['qualifier']=='content' and keyValue[1]['value']!=None)
    # hbase_rdd_content=hbase_rdd_content.mapValues(content_analyse) #按照tf-idf分析去除不相干的关键词以及得到top k的词
    # tags=jieba.analyse.extract_tags(content,top_num)

    '''
    |著名|导演|郭宝昌|最新|执导|的|中国|首部|历史|谋略|情节剧|《|谋圣|鬼谷子|》|正在|浙江省|象山|影视城|热拍|。|郭宝昌|出|“|宅门|”|后|首次|玩|“|谋略|”|，|让|这部|剧|深受|观众|期待|，|他|表示|《|谋圣|鬼谷子|》|要|打|造成|中国|版|《|权力|的|游戏|》|。|

    '''
    # hbase_rdd_new=hbase_rdd_title.union(hbase_rdd_content)
    hbase_rdd_new=hbase_rdd_title
    #hbase_rdd_title=hbase_rdd_title.flatMap(inverted)  

    hbase_rdd_new=hbase_rdd_new.flatMap(inverted).groupByKey()
    #list(set(myList)) 对list去重，一行里面包括多个url并rank
    hbase_rdd_new=hbase_rdd_new.filter(lambda keyValue:len(keyValue[0])>4) #过滤太短的关键词

    #rank策略 content基于tfidf后
    # hbase_rdd_new=hbase_rdd_new.mapValues(lambda v: list(set(v))).mapValues(lambda v: "|".join(v))
    hbase_rdd_new=hbase_rdd_new.mapValues(ridoff).mapValues(lambda v: "|".join(v))

    # sc.union(rdd1, rdd2)
    # output = hbase_rdd_new.collect()
    # for (k, v) in output:
    #     for url in v:
    #         if len(k)>1:
    #             hbaseput(sc,'ubuntu1','test3',[k,'f','index',url])
        # print(k+':'+",".join(v)) #记得删除重复的url


        # if v['qualifier']=='content':
        # print(eval("'%s'"%v['value']))

    # wordRDD=tc.flatMap(lambda x:jieba.cut(x))
    # wordFreRDD=wordRDD.map(lambda x:(x,1))
    # counts=wordFreRDD.reduceByKey(add)

    # tags=jieba.analyse.extract_tags(content,top_num)

    #hbase_outputformat <host> test row1 f q1 value1

    host='ubuntu1'
    table='newsindex'
    confout = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    keyConvout = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConvout = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    #rowid的设计是唯一的，但内容不唯一
    hbase_rdd_new.map(lambda x: [x[0],'f','index',x[1]]).map(lambda x: (x[0], x)).saveAsNewAPIHadoopDataset(
        conf=confout,
        keyConverter=keyConvout,
        valueConverter=valueConvout)
    sc.stop()
    #处理value的内容

'''
result = pairs.filter(lambda keyValue: len(keyValue[1]) < 20)

nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect()
for num in squared:
print "%i " % (num)

pairs = lines.map(lambda x: (x.split(" ")[0], x))
'''
        #print((k, v))
'''
(u'http://www.chinanews.com/yl/2015/12-13/7668707.shtml', {u'qualifier': u'title', u'timestamp': u'1449980290800', 
    u'value': u'\\xE9\\xA6\\x99\\xE6\\xB8\\xAF\\xE6\\xBC\\x94\\xE5\\x91\\x98\\xE6\\x9E\\x97\\xE5\\xAD\\x90\\xE8\\x81\\xAA\\xE5\\xBD\\x93\\xE7\\x88\\xB8\\xE7\\x88\\xB8 \\xE8\\xA2\\xAB\\xE5\\x84\\xBF\\xE5\\xAD\\x90\\xE8\\x84\\x9A\\xE8\\xB8\\xA2\\xE6\\x84\\x9F\\xE5\\x8A\\xA8\\xE5\\x88\\xB0\\xE5\\x93\\xAD(\\xE5\\x9B\\xBE)', u'columnFamily': u'f', u'type': u'Put', u'row': u'http://www.chinanews.com/yl/2015/12-13/7668707.shtml'})


u'row1', {u'qualifier': u'a', u'timestamp': u'1450598363113', u'value': u'value1', u'columnFamily': u'f1', u'type': u'Put', u'row': u'row1'}
(u'row1', {u'qualifier': u'b', u'timestamp': u'1450598369239', u'value': u'value2', u'columnFamily': u'f1', u'type': u'Put', u'row': u'row1'})
(u'row2', {u'qualifier': u'', u'timestamp': u'1450598376945', u'value': u'value3', u'columnFamily': u'f1', u'type': u'Put', u'row': u'row2'})
(u'row3', {u'qualifier': u'', u'timestamp': u'1450598382736', u'value': u'value4', u'columnFamily': u'f1', u'type': u'Put', u'row': u'row3'})
'''