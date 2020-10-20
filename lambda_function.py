import urllib.request
from bs4 import BeautifulSoup
import csv
import pandas as pd
import io
import boto3
import s3fs
import itertools
from linebot import LineBotApi
from linebot.models import TextSendMessage

def lambda_handler(event, context):

    url = 'https://follow.yahoo.co.jp/themes/051839da5a7baa353480'
    html = urllib.request.urlopen(url)
    # htmlパース
    soup = BeautifulSoup(html, "html.parser")


    def news_scraping(soup=soup):
        """
        記事のタイトルとurlを取得
        """
        title_list = []
        titles = soup.select('#wrapper > section.content > ul > li:nth-child(n) > a.detailBody__wrap > div.detailBody__cnt > p.detailBody__ttl')

        for title in titles:
            title_list.append(title.string)

        url_list = []   
        urls = soup.select('#wrapper > section.content > ul > li:nth-child(n) > a.detailBody__wrap')

        for url in urls:
            url_list.append(url.get('href'))

        return title_list,url_list

    def get_s3file(bucket_name, key):
        s3 = boto3.resource('s3')
        s3obj = s3.Object(bucket_name, key).get()

        return io.TextIOWrapper(io.BytesIO(s3obj['Body'].read()))

    def write_df_to_s3(csv_list):

        csv_buffer = io.StringIO()
        csv_list.to_csv(csv_buffer,index=False,encoding='utf-8-sig')
        s3_resource = boto3.resource('s3')
        s3_resource.Object('バケット名','ファイル名').put(Body=csv_buffer.getvalue())

    def send_line(content):
        access_token = ********
        #Channel access token を記入
        line_bot_api = LineBotApi(access_token)
        line_bot_api.broadcast(TextSendMessage(text=content))

    ex_csv =[]
    #前回スクレイピング分のurlを入れる
    for rec in csv.reader(get_s3file('バケット名', 'ファイル名')):
        ex_csv.append(rec)

    ex_csv = ex_csv[1:]
    #index=Falseとして書き込んだはずだが読み込んだcsvの先頭に0というインデックス(?)が書き込まれていた
    ex_csv = list(itertools.chain.from_iterable(ex_csv))
    #読み込んだcsvが二次元配列になっていたため一次元に変換

    title,url = news_scraping()
    csv_list = url

    #ex_csvと比較して更新分を取り出す
    for i in range(20):
        #完全一致してくれなかったためinを使用
        if csv_list[i] in ex_csv[0]:
            num = i
        #ex_csvの最新記事がcsv_listの何番目の記事に当たるか調べる
            break
        else:
            num = 'all'

    if num == 'all':
        send_list = [None]*2*20
        send_list[::2] = title
        send_list[1::2] = url
        send_list = "\n".join(send_list)
    #タイトル、urlを交互に挿入し、改行

    elif num == 0:
        send_list = '新しいニュースはありません'

    else:
        send_list = [None]*2*num
        send_list[::2] = title[:num]
        send_list[1::2] = url[:num]
        send_list = "\n".join(send_list)
    ##タイトル、urlを交互に挿入し、改行

    send_line(send_list)

    csv_list = pd.DataFrame(csv_list)
    #リストのままS3に書き込むとエラーが出るためデータ型を変換
    write_df_to_s3(csv_list)
    #S3にcsv_listを書き込んで終了
