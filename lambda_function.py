import json
import boto3

from email_parsing import getEmail
from ai_parsing import ai_parsing
from quote_lanes import quote_lanes
from email_send import send_email
from sns_send import sns_send

ses = boto3.client('ses',region_name="us-east-1")

def lambda_handler(event, context):
    
    emailObject, key, token, customer = getEmail(event)
    
    if key is None:
        
        return {
            'statusCode': 200,
            'body': json.dumps("Customer not set up for email quoting.")
        }
    
    quote_object = ai_parsing(emailObject)
    
    if 'statusCode' in quote_object:
        
        return quote_object
    
    restructuredObjects = quote_lanes(quote_object, emailObject, key, token)
    
    send_email(restructuredObjects, emailObject)
    
    sns_send(restructuredObjects, emailObject)
    
    return {
        'statusCode': 200,
        'body': json.dumps(restructuredObjects, default=str)
    }

