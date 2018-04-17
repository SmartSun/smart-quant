import boto3
import datetime
import requests
import json
from common_funcs import get_soup
from settings import API_KEY


def get_total_shares(symbol):
    url = 'https://www.nasdaq.com/symbol/%s/institutional-holdings' % symbol
    soup = get_soup(url)
    table = soup.find_all('table')[5]
    total_shares = int(table.find_all('td')[1].text.replace(',', '')) * 1000000
    return total_shares


def main(symbol, date):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('smart-quant-data')
    quarter_date_mapping = {
        1: '03-31',
        2: '06-30',
        3: '09-30',
        4: '12-31'
    }
    start_dt = datetime.date(*map(int, date.split('-'))) - datetime.timedelta(days=90)
    start_year = start_dt.year
    start_date = '%s-%s' % (start_year, quarter_date_mapping[start_dt.month // 3])

    total_shares = get_total_shares(symbol)
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s&apikey=%s'
    url = url % (symbol.upper(), API_KEY)
    json_content = json.loads(requests.get(url).content)
    output = []
    for d, history in json_content['Time Series (Daily)'].iteritems():
        if d > start_date and d <= date:
            output.append('\t'.join([
                d, symbol, history['1. open'], history['2. high'],
                history['3. low'], history['4. close'], history['5. volume'],
                str(total_shares)
            ]))
    body = '\n'.join(output) + '\n'
    key = 'symbol_history/%s/%s/%s.txt' % (date, symbol, symbol)
    bucket.put_object(Key=key, Body=body)
