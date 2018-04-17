import boto3
import json


def get_symbol_list():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='smart-quant-data', Key='symbol_list/symbol_list')
    return obj['Body'].read().split('\n')


def lambda_handler(event, context):
    date = event['date']
    scraping_type = event['type']
    sns = boto3.client('sns')
    arn = 'arn:aws:sns:us-east-1:602379591537:smart-quant-symbol'
    symbol_list = filter(None, get_symbol_list())
    for symbol in symbol_list:
        sns.publish(
            TargetArn=arn,
            Message=json.dumps({'symbol': symbol, 'date': date, 'type': scraping_type})
        )
