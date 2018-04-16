import boto3
import requests
import json
import uuid
from bs4 import BeautifulSoup
import datetime
from retrying import retry


def get_soup(url):
    res = requests.get(url)
    return BeautifulSoup(res.content, 'lxml')


@retry(stop_max_attempt_number=5)
def parse_page(url):
    soup = get_soup(url)
    table = soup.find_all('table')[8]
    for tr in table.find_all('tr'):
        tds = tr.find_all('td')
        if not tds:
            continue
        owner_code = tds[0].find('a')['href'].split('/')[-1]
        date = str(datetime.datetime.strptime(tds[1].text, '%m/%d/%Y').date())
        held_share = int(tds[2].text.replace(',', ''))
        change = int(tds[3].text.replace(',', '').replace(')', '').replace('(', '-'))
        yield (owner_code, date, held_share, change)


def lambda_handler(event, context):
    msg = json.loads(event['Records'][0]['Sns']['Message'])
    output = {}
    try:
        for item in parse_page(msg['url']):
            l = output.setdefault(item[1], [])
            l.append(item)
    except:
        return

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('smart-quant-data')
    for date, content in output.iteritems():
        body = '\n'.join(['\t'.join(i) for i in content]) + '\n'
        file_name = str(uuid.uuid1())
        key = 'institutional_holdings/%s/%s/%s.txt' % (date, msg['symbol'], file_name)
        bucket.put_object(Key=key, Body=body)
