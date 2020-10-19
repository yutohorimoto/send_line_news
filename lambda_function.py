import urllib.request
from bs4 import BeautifulSoup
import csv
import pandas as pd
from io import StringIO
import io
import boto3
import s3fs
import time
import itertools
from linebot import LineBotApi
from linebot.models import TextSendMessage

def lambda_handler(event, context):
    # TODO implement
    url = 'https://follow.yahoo.co.jp/themes/051839da5a7baa353480'
    
    #htmlの取得
    html = urllib.request.urlopen(url)
    
    # htmlパース
    soup = BeautifulSoup(html, "html.parser")
    
    
    
    def extract_pick_up(soup=soup):
        """
        指定されたページのhtmlを読み込み、最新記事を抜粋してくる
        """
        title_list = []
        titles = soup.select('#wrapper > section.content > ul > li:nth-child(n) > a.detailBody__wrap > div.detailBody__cnt > p.detailBody__ttl')
    
        for title in titles:
            title_list.append(title.string)
    
    
        url_list = []
    
        links = soup.select('#wrapper > section.content > ul > li:nth-child(n) > a.detailBody__wrap')
        
        for link in links:
            url_list.append(link.get('href'))
        
        #print(li_list[0],url_list[0])
        return title_list,url_list
    
    #def extract_update_article(title,url):
        
    def get_s3file(bucket_name, key):
        s3 = boto3.resource('s3')
        s3obj = s3.Object(bucket_name, key).get()
    
        return io.TextIOWrapper(io.BytesIO(s3obj['Body'].read()))
        
    def write_df_to_s3(csv_list):
   
        csv_buffer = StringIO()
        csv_list.to_csv(csv_buffer,index=False,encoding='utf-8-sig')
        #csv_list.to_csv(csv_buffer,encoding='utf-8-sig')
        s3_resource = boto3.resource('s3')
        s3_resource.Object('sendline','news.csv').put(Body=csv_buffer.getvalue())
    
    def send_line(content):
        access_token = ********
        line_bot_api = LineBotApi(access_token)
        line_bot_api.broadcast(TextSendMessage(text=content))
    
    ex_csv =[]
    
    for rec in csv.reader(get_s3file('sendline', 'news.csv')):
        ex_csv.append(rec)
    
    ex_csv = ex_csv[1:]
    ex_csv = list(itertools.chain.from_iterable(ex_csv))
    print(ex_csv[0])
    
    #s3 = boto3.client('s3')
    #obj = s3.get_object(Bucket='sendline', Key='news.csv')
    #body = obj['Body']
    #csv_string = body.read().decode('utf-8')
    #df = pd.read_csv(StringIO(csv_string))
    #print(df)
    
    

    #if __name__ == "__main__":
    title,url = extract_pick_up()
    csv_list = url
    
    #ex_csvと比較して更新分を取り出す
    if csv_list[0] in ex_csv[0]:
        print('true')
    for i in range(20):
        #完全一致してくれなかったため
        if csv_list[i] in ex_csv[0]:
            num = i
            break
        else:
            num = 'all'

    if num == 'all':
        send_list = [None]*2*20
        send_list[::2] = title
        send_list[1::2] = url
        send_list = "\n".join(send_list)
    
    elif num == 0:
        send_list = '新しいニュースはありません'
    
    else:
        send_list = [None]*2*num
        send_list[::2] = title[:num]
        send_list[1::2] = url[:num]
        send_list = "\n".join(send_list)
    
    send_line(send_list)
    
    csv_list = pd.DataFrame(csv_list)
    write_df_to_s3(csv_list)
    #print(title[0],url[0])
    #return title,url