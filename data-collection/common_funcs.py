from bs4 import BeautifulSoup
import requests


def get_soup(url):
    res = requests.get(url)
    return BeautifulSoup(res.content, 'html.parser')
