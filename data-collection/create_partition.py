from __future__ import print_function

import boto3


def lambda_handler(event, context):
    key = event['Records'][0]['s3']['object']['key']
    athena = boto3.client('athena')

    if key.startswith('institutional_holdings/'):
        date, symbol = key.split('/')[1:3]
        query = """
            ALTER TABLE institutional_holdings
            ADD IF NOT EXISTS PARTITION
            (date_p='%(date)s', symbol_p='%(symbol)s')
            LOCATION 's3://smart-quant-data/institutional_holdings/%(date)s/%(symbol)s/'
        """ % {'date': date, 'symbol': symbol}
        database = 'institutions'

    if key.startswith('symbol_history/'):
        date, symbol = key.split('/')[1:3]
        query = """
            ALTER TABLE institutional_holdings
            ADD IF NOT EXISTS PARTITION
            (date_p='%(date)s', symbol_p='%(symbol)s')
            LOCATION 's3://smart-quant-data/symbol_history/%(date)s/%(symbol)s/'
        """ % {'date': date, 'symbol': symbol}
        database = 'symbols'

    return athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://aws-athena-query-results-602379591537-us-east-1',
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )
