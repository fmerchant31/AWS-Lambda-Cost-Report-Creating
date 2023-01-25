import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from premailer import transform

from json_logger import logger

# This address must be verified with Amazon SES.
# Reading from Lambda Env vars
SENDER = os.environ['sender_email_id']

# If your account is still in the sandbox, this address must be verified.
# Reading from Lambda Env vars
RECIPIENT = os.environ['recipient_email_id']

# If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES. Reading from Lambda Env vars
AWS_REGION = os.environ['aws_region']

# The subject line for the email.
SUBJECT = f"{os.environ['subject']} - {datetime.utcnow().date()}"


def send_email(html_body):
    """

    :param html_body:
    :return:
    """
    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Mail clients usually strip styles from HTML body. Using premailer to add the styles inline
    # Tested with Gmail Web Client
    html_body_transformed = transform(html_body)
    logger.info('Transformed HTML', extra=dict(data={"INFO": html_body_transformed}))
    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION)
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': html_body_transformed,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        msg = {"ERROR": e.response['Error']['Message']}
        logger.error('Email client error', extra=dict(data=msg))
        raise e
    else:
        msg = {"SUCCESS": f"Email sent! Message ID: {response['MessageId']}"}
        logger.info('Email Log', extra=dict(data=msg))
