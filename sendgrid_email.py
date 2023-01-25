# using SendGrid's Python Library
# https://github.com/sendgrid/sendgrid-python
import os
from datetime import datetime

from premailer import transform
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, From

from json_logger import logger


def send_email(html_body, api_key):
    """
    Function which sends email using sendgrid
    Args:
        html_body:
        api_key:

    Returns:

    """
    # Transform HTML body to convert to inline CSS styles as email client strip
    html_body_transformed = transform(html_body)

    # Form a list of of email IDs from comma separated env vars. Strip them of any whitespaces
    weekly_recipient_email_ids_list = [email_id.strip() for email_id in
                                       os.environ['weekly_recipient_email_ids'].split(',')]
    daily_recipient_email_ids_list = [email_id.strip() for email_id in
                                      os.environ['daily_recipient_email_ids'].split(',')]

    # Send email to weekly recipients on Monday. And to Daily recipients on other days 
    to_emails = weekly_recipient_email_ids_list if datetime.utcnow().weekday() == 0 else daily_recipient_email_ids_list
    message = Mail(
        from_email=From(os.environ['sender_email_id'], os.environ['sender_name']),
        to_emails=to_emails,
        subject=f'{os.environ["subject"]} - {datetime.utcnow().date()}',
        html_content=html_body_transformed)

    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        logger.info(f'Sendgrid response status code: {response.status_code}')
        logger.info(f'Sendgrid response body: {response.body}')
        logger.info(f'Sendgrid response headers: {response.headers}')
        return response
    except Exception as e:
        logger.error(f'Sendgrid API failed: {str(e)}')
