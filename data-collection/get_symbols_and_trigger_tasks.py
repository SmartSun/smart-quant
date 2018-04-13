import boto3

sns = boto3.client('sns')
symbol_list = ['mu', 'fb', 'yy', 'jd', 'momo', 'snap', 'wb', 'expe', 'ntes']


def lambda_handler(event, context):
    # TODO implement
    arn = 'arn:aws:sns:us-east-1:602379591537:smart-quant-symbol'
    for symbol in symbol_list:
        sns.publish(
            TargetArn=arn,
            Message=symbol
        )
