import json
import boto3
import time
import email.parser as ep
import base64
import re
from email.policy import default
from email.utils import parseaddr
from customer_lookup import customerLookup
from ai_parsing import claude_ocr
s3 = boto3.client('s3')
brt = boto3.client(service_name='bedrock-runtime')

CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')

schema = {
    "senderName": {
        "type": "string",
        "description": "The name of the person who sent the email."
    },
    "senderEmail": {
        "type": "string",
        "description": "The email address of the person who sent the email."
    }
}

def findnth(string, substring, n):
   parts = string.split(substring, n + 1)
   if len(parts) <= n + 1:
      return -1
   return len(string) - len(parts[-1]) - len(substring)
   
def cleanhtml(raw_html):
    cleantext = re.sub(CLEANR, '', raw_html)
    return cleantext

def getEmail(event):
    """ This function takes the event that triggered the lambda function and 
    grabs the email file, and parses it out. Overall it returns the date the
    email was received, the body of the email, the name of the sender, and the
    email of the sender.
    
    If an email is forwarded, the function takes the beginning of the email body,
    and passes it to Claude to figure out who the original sender was.
    """
    
    print("Incoming event is:", event) # grabbing timestamps
    current_timestamp = time.time()
    formatted_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    
    bucket = event['Records'][0]['s3']['bucket']['name'] # grab bucket and key from s3 put event
    key = event['Records'][0]['s3']['object']['key']
    
    response = s3.get_object(Bucket=bucket, Key=key) # get email object
    emailParser = ep.BytesParser(policy=default) # initialize BytesParser class
    msg = emailParser.parsebytes(response['Body'].read()) # parse out the email
    
    date = msg['Date'] # Extract date the email was sent
    subject = msg['Subject'] # Extract the subject of the email
    body = ""
    types = []
    urls = []
    
    if msg.is_multipart(): # Iterate over the parts of the email and grab the body of the email
        for part in msg.walk():
            types.append(part.get_content_type())
        for part in msg.walk():
            
            encoding = part.get('Content-Transfer-Encoding')
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))
                
            if ctype == 'text/plain' and 'attachment' not in cdispo and encoding != "base64":
                body = body + part.get_body().as_string() + "\n"
                
            elif ctype == 'text/plain' and 'attachment' not in cdispo and encoding == "base64":
                nth = findnth(part.get_body().__str__(), "\n", 2)
                body = body + base64.b64decode(part.get_body().__str__()[nth:]).decode() + "\n"
            
            elif ctype == 'text/html' and 'text/plain' not in types:
                raw_html = part.get_body().as_string()
                index = raw_html.find("<!doctype html>")
                if index == -1:
                    index = raw_html.find("<html>")
                try:
                    left_url_index = raw_html.find("https://customertms")
                    right_url_index = raw_html[left_url_index:].find(">")
                    url_string = raw_html[left_url_index:left_url_index+right_url_index-1]
                    urls.append(url_string)
                except:
                    print("no")
                raw_html = raw_html[index:]
                clean_text = cleanhtml(raw_html)
                body = body + clean_text + "\n"
            
            elif ctype in ['image/jpeg', 'image/png', 'image/webp']:
                
                image = base64.b64encode(part.get_payload(decode=True)).decode('utf-8')
                image_as_string = claude_ocr(image, ctype)
                body = body + image_as_string + "\n"
            
                
    else:
        encoding = msg.get('Content-Transfer-Encoding')
        ctype = msg.get_content_type()
        cdispo = str(msg.get('Content-Disposition'))
        
        if ctype == 'text/plain' and 'attachment' not in cdispo and encoding != "base64":
            body = body + msg.get_body().as_string() + "\n"
                
        elif ctype == 'text/plain' and 'attachment' not in cdispo and encoding == "base64":
            nth = findnth(msg.get_body().__str__(), "\n", 2)
            body = body + base64.b64decode(msg.get_body().__str__()[nth:]).decode()
            
        elif ctype == 'text/html':
            raw_html = msg.get_body().as_string()
            index = raw_html.find("<!doctype html>")
            if index == -1:
                index = raw_html.find("<html>")
            try:
                left_url_index = raw_html.find("https://customertms")
                right_url_index = raw_html[left_url_index:].find(">")
                url_string = raw_html[left_url_index:left_url_index+right_url_index-1]
                urls.append(url_string)
            except:
                print("no")
            raw_html = raw_html[index:]
            clean_text = cleanhtml(raw_html)
            body = body + clean_text + "\n"
            
        elif ctype in ['image/jpeg', 'image/png', 'image/gif', 'image/webp']:
            image = base64.b64encode(msg.get_payload(decode=True)).decode('utf-8')
            image_as_string = claude_ocr(image, ctype)
            body = body + image_as_string + "\n"

    body = subject + "\n" + body
    
    if "Fwd" in subject:
        
        beginningOfBody = body.split("---------- Forwarded message ---------")[1]
        beginningOfBody = beginningOfBody.split("Subject")[0]

        haiku = 'anthropic.claude-3-haiku-20240307-v1:0'
    
        response = brt.invoke_model(
                modelId=haiku,
                body=json.dumps(
                    {
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": 4096,
                        "temperature": 0,
                        "system": f"You are a helpful AI assistant who parses out the sender of an email from a body of text according to the following schema:\n{schema}",
                        "messages": [
                            {
                                "role": "user",
                                "content": [{"type": "text", "text": f"Could you figure out who sent this email? If the email is a No Reply email, return the name as \"No Reply\" and return the email address as is.\n{beginningOfBody}"}]
                            }
                        ]
                    }
                ),
        )
        
        result = json.loads(response.get("body").read())
        output_list = result.get("content", [])
        
        leftBracket = output_list[0]['text'].find("{")
        rightBracket = output_list[0]['text'].rfind("}")
        object = json.loads(output_list[0]['text'][leftBracket:rightBracket+1])
        
        customerName = object['senderName']
        customerEmail = object['senderEmail']
        
        subject = subject.split("Fwd: ")[1]
        forwarded = True
        
    else:
        
        customerName, customerEmail = parseaddr(msg['From'])
        forwarded = False
    
    senderName, senderEmail = parseaddr(msg['From'])
    
    try:
        threadId = msg['Thread-ID']
    except:
        threadId = None
        
    try:
        inReplyTo = msg['In-Reply-To']
    except:
        inReplyTo = None

    try:
        references = msg['References'].split(" ")
    except:
        references = None
    
    try:
        originalMessageId = msg['originalMessageId']
    except:
        originalMessageId = None
    
    key, token, customer = customerLookup(customerEmail)
    
    if ("Re:" in subject):
        subject = subject.split("Re: ")[1]
    
    if ("RE:" in subject):
        subject = subject.split("RE: ")[1]
    
    
    if key is None:
        
        emailInfo = {
            "date": date,
            "subject": subject,
            "senderName": senderName,
            "senderEmail": senderEmail,
            "customerName": customerName,
            "customerEmail": customerEmail,
            "body": body,
            "messageID": msg['Message-ID'],
            "threadID": threadId,
            "inReplyTo": inReplyTo,
            "references": references,
            "originalMessageId": originalMessageId,
            "forwarded": forwarded
        }
        
        print(f"Incoming quote from {emailInfo['customerEmail']}. \n{emailInfo['body']}")
        
        return emailInfo, None, None, None
        
    else:
        
        if customer['name'] == "CUSTOMER_3":
            
            emailInfo = {
                "date": date,
                "subject": subject,
                "senderName": senderName,
                "senderEmail": senderEmail,
                "customerName": customerName,
                "customerEmail": customerEmail,
                "customerCompany": customer['name'],
                "tproId": customer['tproId'],
                "accountManager": customer['accountManager'],
                "clientStrategyManager": customer['clientStrategyManager'],
                "defaultEquipment": customer['defaultEquipment'],
                "body": body,
                "messageID": msg['Message-ID'],
                "threadID": threadId,
                "inReplyTo": inReplyTo,
                "references": references,
                "originalMessageId": originalMessageId,
                "forwarded": forwarded,
                "url": urls[0]
            }
            
            
        else:
            emailInfo = {
                "date": date,
                "subject": subject,
                "senderName": senderName,
                "senderEmail": senderEmail,
                "customerName": customerName,
                "customerEmail": customerEmail,
                "customerCompany": customer['name'],
                "tproId": customer['tproId'],
                "accountManager": customer['accountManager'],
                "clientStrategyManager": customer['clientStrategyManager'],
                "defaultEquipment": customer['defaultEquipment'],
                "body": body,
                "messageID": msg['Message-ID'],
                "threadID": threadId,
                "inReplyTo": inReplyTo,
                "references": references,
                "originalMessageId": originalMessageId,
                "forwarded": forwarded
            }
        
        print(f"Incoming quote from {emailInfo['customerEmail']}. \n{emailInfo['body']}")
    
        return emailInfo, key, token, customer
        