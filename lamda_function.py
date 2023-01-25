import json
import os
from cost_explorer import CostExplorer
from json_logger import logger
from sendgrid_email import send_email
from aws_secret import get_secret


def lambda_handler(event, context):
    try:
        secret = get_secret()
        secret = json.loads(secret)
        api_key = secret[os.environ['secret_key']]
        ce = CostExplorer()
        html_report = ce.generate_report()
        send_email(html_report, api_key)
        msg = f'Lambda function {context.function_name} ran successfully'
        logger.info(msg)
        return {
            'statusCode': 200,
            'body': msg
        }
    except Exception as e:
        msg = f'Failed to generate report due to {str(e)}'
        logger.error(msg)
        return {
            'statusCode': 500,
            'body': msg
        }
