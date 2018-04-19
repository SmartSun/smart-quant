from __future__ import print_function

import json
import parse_symbol_institutional_holdings_mainpage
import parse_symbol_history


def lambda_handler(event, context):
    eb = json.loads(event['Records'][0]['Sns']['Message'])
    symbol = eb['symbol']
    date = eb['date']
    scraping_type = eb['type']

    if scraping_type == 'holdings':
        parse_symbol_institutional_holdings_mainpage.main(symbol, date)
    if scraping_type == 'history':
        parse_symbol_history.main(symbol, date)
