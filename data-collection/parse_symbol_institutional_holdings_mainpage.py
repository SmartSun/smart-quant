from __future__ import print_function

import json
import boto3
import requests
from bs4 import BeautifulSoup


def get_soup(url):
    res = requests.get(url)
    return BeautifulSoup(res.content, 'html.parser')


def get_max_page(base_url):
    soup = get_soup(base_url)
    hrefs = soup.find_all('a', href=True)
    pages = []
    for a in hrefs:
        if a['href'].startswith(base_url + '?page='):
            pages.append(int(a['href'].split('=')[-1]))
    return max(pages)


def lambda_handler(event, context):
    sns = boto3.client('sns')
    # print("Received event: " + json.dumps(event, indent=2))
    symbol = event['Records'][0]['Sns']['Message']
    base_url = 'https://www.nasdaq.com/symbol/%s/institutional-holdings' % symbol
    max_page = get_max_page(base_url)
    arn = 'arn:aws:sns:us-east-1:602379591537:smart-quant-institutional-holdings-url'
    for page in range(1, max_page + 1):
        page_url = base_url + '?page=%s' % page
        sns.publish(
            TargetArn=arn,
            Message=json.dumps({'url': page_url, 'symbol': symbol})
        )
