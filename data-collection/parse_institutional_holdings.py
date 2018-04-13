import requests
from bs4 import BeautifulSoup
import datetime
import pandas as pd


def get_soup(url):
    res = requests.get(url)
    return BeautifulSoup(res.content, 'lxml')


def get_max_page(base_url):
    soup = get_soup(base_url)
    hrefs = soup.find_all('a', href=True)
    pages = []
    for a in hrefs:
        if a['href'].startswith(base_url + '?page='):
            pages.append(int(a['href'].split('=')[-1]))
    return max(pages)


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


def main(symbol):
    base_url = 'https://www.nasdaq.com/symbol/%s/institutional-holdings' % symbol
    max_page = get_max_page(base_url)
    output = []
    for page in range(1, max_page + 1):
        for item in parse_page(base_url + '?page=%s' % page):
            output.append(item)
    df = pd.DataFrame.from_records(output, columns=['owner_code', 'date', 'held_shares', 'change'])
    df.to_csv('%s.txt' % symbol, sep='\t')


main('baba')
