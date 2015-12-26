#-*-coding:utf-8-*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
__author__ = 'kira'
from flask import Flask, request, make_response,render_template,session,redirect,url_for,flash
from flask.ext.wtf import Form
from wtforms import StringField,SubmitField
# from wtforms.validators import DataRequired
import happybase
# conn=happybase.Connection('10.141.211.91')

class SearchForm(Form):
    name=StringField('搜索关键词:')
    submit=SubmitField('点击搜索')


def geturl(conn,rowid):
    table=conn.table('newsindex')
    print table
    # a=geturl(test3.decode('gbk').encode('utf-8')) windows console编码输入
    row=table.row(rowid)
    rows=table.scan(row_prefix=rowid)
    print rows
    if 'f:index' in row:
        result= row['f:index'] .split('|')
        return result #http://www.chinanews.com/cj/2015/12-13/7668505.shtml|http://www.chinanews.com/sh/2015/12-13/7668690.shtml
    else:
        return None

def getcontent(conn,url):
    table=conn.table('news')
    article=table.row(url)
    return article  #dict类型
'''
{'f:time': '2015-12-13 05:28:00', 'f:content': '\xe3\x83\xe5\xb0\x8f\xe6\x97', 'f:title': '\xe7\x8f\xa0\xe4\xb8\x89\xe8\xa7\x92}
'''
app = Flask(__name__)
app.config['SECRET_KEY']='guochenghao'
conn=happybase.Connection('10.141.211.91')

@app.route('/',methods=['GET','POST'])
def search():
    print conn
    # conn.open()
    form=SearchForm()
    backlist=[]
    if form.validate_on_submit():
        keyword=form.name.data
        print keyword
        if keyword!='':
            listurl=geturl(conn,keyword.decode('utf-8').encode('utf-8'))
            if listurl!=None:
                for url in listurl:
                    # print url
                    urlcontent=getcontent(conn,url)
                    keyvalue=(url,urlcontent)
                    backlist.append(keyvalue)
    return render_template('search.html',backlist=backlist,form=form)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
