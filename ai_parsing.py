import json
import boto3
import os
from pymongo import MongoClient

CONNECTION_STRING = os.environ['CONNECTION_STRING']
client = MongoClient(host=CONNECTION_STRING)
loadDb = client.LoadDetail

brt = boto3.client(service_name='bedrock-runtime')

common_equipment_types = ['Dry Van', 'Van', 'Reefer Van', 'Refrigerated Trailer', 'Flatbed', 'Conestoga', 'Stepdeck', 'Sprinter', 'Sprinter Van', 'Cargo Van', 'Box Truck', 'Flatbed Hotshot']

schema = {
    "stops": {
        "type": "array",
        "description": "Every load of freight has at least two stops, a pickup and a delivery, these can also be known as \"Ship From\" and \"Ship To\" locations. Sometimes there are intermediate stops which can be either extra pickup locations, or extra delivery locations. This field should be an array of strings, where each string is the location information (such as city, the unabbreviated state name, zip, address, and country) for each stop. The first entry should be the first pickup location (or the Ship From location) and the last entry should be the final delivery location (or the Ship To location). Do not include any date or time information in this field. It should only be location information"
    },
    "equipmentType": {
        "type": "string",
        "description": f"This field details the type of trailer required to haul the load of freight. Note: If a temperature is specified, or if says the load is Frozen, you can assume Reefer. See here for common equipment types: {common_equipment_types}"
    },
    "pickupDate": {
        "type": "string",
        "description": "The date that the freight needs to be picked up. If not specified, or if the date is to be determined, put \"\". Please format in YYYY-mm-dd format."
    },
    "deliveryDate": {
        "type": "string",
        "description": "The date that the freight needs to be delivered by. If not specified, or if the date is to be determined, \"\". Please format in YYYY-mm-dd format. "
    },
    "importantNotes": {
        "type": "string",
        "description": "Other vital information about the load. This can include whether the load needs to be tarped, or if a driver needs a TWIC card, etc."
    },
    "shipmentType": {
        "type": "string",
        "description": "Whether or not the load is full truckload or less-than-truckload. Sometimes, less-than-truckload is also referred to as a \"partial\". \"FTL\" for full truckload and \"LTL\" for less-than-truckload."
    },
    "isHazmat": {
        "type": "string",
        "description": "Whether or not there are hazardous materials on the load that would require a hazmat certification.Sometimes, hazardoues materials are just shortened to \"haz\", this means the load is hazmat. Sometimes it won't say directly that the shipment is hazmat, but they will give a UN number. A UN number means that the load is hazmat. Only return either the value \"True\" or the value \"False\". If no information regarding hazardous materials is given, assume \"False\""
    },
    "unNumber": {
      "type": "string",
      "description": "A UN number is a number used to classify freight that contains hazardous material. If the load contains hazardous material, the email might also give a 4 digit number. This field should be the 4 digit number, including any leading zeros."
    },
    "tankerEndorsement": {
        "type": "string",
        "description": "Whether or not the load requires a tanker endorsement. Possible values are \"True\" and \"False\". If not specified, assume \"False\""
    },
    "teamRequired": {
        "type": "string",
        "description": "Whether or not a team of drivers is required. Possible values are \"True\" and \"False\". If not specified, assume \"False\""
    },
    "weight":{
        "type": "number",
        "description": "The weight of the load that needs to be picked up, in pounds. It is possible that this will not be given. If not given, assume 40000."
    },
    "senderName": {
        "type": "string",
        "description": "The name of whoever sent the request for quote."
    },
    "senderEmail": {
        "type": "string",
        "description": "The email of whoever sent the request for quote."
    },
    "senderCompany": {
        "type": "string",
        "description": "The name of the company the sender works for."
    },
    "pieces": {
        "type": "string",
        "description": "The quantity and units being moved, this could be a number of pallets, a number of containers, etc. If this is not specified, put \"\""
    }
}

specific_schema = {
    "stops": {
        "type": "array",
        "description": "Every load of freight has at least two stops, a pickup and a delivery, these can also be known as \"Ship From\" and \"Ship To\" locations. Sometimes there are intermediate stops which can be either extra pickup locations, or extra delivery locations. This field should be an array of strings, where each string is the location information (such as city, the unabbreviated state name, zip, address, and country) for each stop. The first entry should be the first pickup location (or the Ship From location) and the last entry should be the final delivery location (or the Ship To location). Do not include any date or time information in this field. It should only be location information"
    },
    "equipmentType": {
        "type": "string",
        "description": f"This field details the type of trailer required to haul the load of freight. Note: If a temperature is specified, you can assume Reefer. See here for common equipment types: {common_equipment_types}"
    },
    "pickupDate": {
        "type": "string",
        "description": "The date that the freight needs to be picked up. If not specified, or if the date is to be determined, put \"\". Please format in YYYY-mm-dd format."
    },
    "deliveryDate": {
        "type": "string",
        "description": "The date that the freight needs to be delivered by. If not specified, or if the date is to be determined, \"\". Please format in YYYY-mm-dd format. "
    },
    "importantNotes": {
        "type": "string",
        "description": "Other vital information about the load. This can include whether the load needs to be tarped, or if a driver needs a TWIC card, etc."
    },
    "shipmentType": {
        "type": "string",
        "description": "Whether or not the load is full truckload or less-than-truckload. Sometimes, less-than-truckload is also referred to as a \"partial\". \"FTL\" for full truckload and \"LTL\" for less-than-truckload."
    },
    "isHazmat": {
        "type": "string",
        "description": "Whether or not there are hazardous materials on the load that would require a hazmat certification.Sometimes, hazardoues materials are just shortened to \"haz\", this means the load is hazmat. Sometimes it won't say directly that the shipment is hazmat, but they will give a UN number. A UN number means that the load is hazmat. Possible values are \"True\" and \"False\". If not specified, and no UN number is given, assume \"False\""
    },
    "unNumber": {
      "type": "string",
      "description": "A UN number is a number used to classify freight that contains hazardous material. If the load contains hazardous material, the email might also give a 4 digit number. This field should be the 4 digit number, including any leading zeros."
    },
    "tankerEndorsement": {
        "type": "string",
        "description": "Whether or not the load requires a tanker endorsement. Possible values are \"True\" and \"False\". If not specified, assume \"False\""
    },
    "teamRequired": {
        "type": "string",
        "description": "Whether or not a team of drivers is required. Possible values are \"True\" and \"False\". If not specified, assume \"False\""
    },
    "weight":{
        "type": "number",
        "description": "The weight of the load that needs to be picked up, in pounds. It is possible that this will not be given. If not given, assume 40000."
    },
    "senderName": {
        "type": "string",
        "description": "The name of whoever sent the request for quote."
    },
    "senderEmail": {
        "type": "string",
        "description": "The email of whoever sent the request for quote."
    },
    "senderCompany": {
        "type": "string",
        "description": "The name of the company the sender works for."
    },
    "pieces": {
        "type": "string",
        "description": "The quantity and units being moved, this could be a number of pallets, a number of containers, etc. If this is not specified, put \"\""
    },
    "request_number": {
        "type": "number",
        "description": "The freight rate request number (or #)."
    },
    "bid_duration": {
        "type": "string",
        "description": "This will be a string with two dates in it. It will look like \"%d/%mm/%YYYY through %d/%mm/%YYYY\""
    }
}

def claude_ocr(image, image_type):
    """ Sometimes quote requests come over as images embedded into an email, so 
    we can use claude as an Optical Character Recognition model to extract the 
    text from those emails."""
    
    model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    
    response = brt.invoke_model(
        modelId = model_id,
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "temperature": 0,
            "system": "You are being assigned to work as an optical character recognition model. We will give you an image and you need to return the text from the image as a string. Please return the text from the document as a string, exactly as it appears in the document. Be sure to extract any information regarding pickup or delivery locations, also known as Ship From and Ship To Locations. Then, surround the text from the table with \"&\" on both ends.  If there is no text in the image, return an empty string",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": image_type,
                                "data": image,
                            },
                        },
                    ],
                }
            ],
        })
    )
    
    result = json.loads(response.get("body").read())
    output_list = result.get("content", [])
    
    #print(output_list[0]['text'])
    
    if output_list[0]['text'] == "":
        text = ""
    else:
        try:
            text = output_list[0]['text'].split("&")[1]
        except:
            text = ""
    
    return text
    
def check_if_quoted(emailObject):
    """ Checks if we have already quoted this quote request by looking for matching message ID/
    threadIDs/inReplyTo IDs."""
    
    messageID = emailObject['messageID']
    threadID = emailObject['threadID']
    inReplyTo = emailObject['inReplyTo']
    references = emailObject['references']
    originalMessageID = emailObject['originalMessageId']
    
    if references is not None:
        checks = [messageID, threadID, inReplyTo, originalMessageID] + references
    else:
        checks = [messageID, threadID, inReplyTo, originalMessageID, references] 
    to_check = []
    
    for check in checks:
        if check is not None:
            to_check.append(check)
        

    response = loadDb['EmailQuotes'].find({
        "emailData.messageID": {
            "$in": to_check
        },
        "emailData.subject": {
            "$eq": emailObject['subject']
        }
    },
    {
        "emailData": 1
    })
    
    quotes = []
    for quote in response:
        quotes.append(quote)
        
    if len(quotes) > 0:
        return True
    else: 
        return False

def ai_parsing(emailObject):
    """ This function passes the body of the email object to an AI which then
    parses out the body of the email into our desired format. It returns a list
    of dicts.
    """
    
    haiku = 'anthropic.claude-3-haiku-20240307-v1:0'
    
    check = check_if_quoted(emailObject)

    if check:
        
        print("Already quoted")
        return {
            'statusCode': 200,
            'body': json.dumps("Already quoted")
        }
        
    else:
        
        if emailObject['customerCompany'] == 'specific customer':
        
            response = brt.invoke_model(
                    modelId=haiku,
                    body=json.dumps(
                        {
                            "anthropic_version": "bedrock-2023-05-31",
                            "max_tokens": 4096,
                            "temperature": 0,
                            "system": f"You are a helpful AI Assistant tasked with assisting in parse freight movement data. A customer will send over information on a freight shipment they need moved from one place to another, you will need to parse out this data according to the following schema:\n{specific_schema}\n Sometimes, a customer sends information on more than one freight shipment. In this case, make a json object for each shipment and put them all into an array.",
                            "messages": [
                                {
                                    "role": "user",
                                    "content": [{"type": "text", "text": f"Could you parse out the following freight quote? Please be sure to use proper json formatting. Please ignore any information in email signatures from people who work at Circle Logistics, Inc..\n{emailObject['body']}"}]
                                }
                            ]
                        }
                    ),
                )
        
        else:
        
            response = brt.invoke_model(
                    modelId=haiku,
                    body=json.dumps(
                        {
                            "anthropic_version": "bedrock-2023-05-31",
                            "max_tokens": 4096,
                            "temperature": 0,
                            "system": f"You are a helpful AI Assistant tasked with assisting in parse freight movement data. A customer will send over information on a freight shipment they need moved from one place to another, you will need to parse out this data according to the following schema:\n{schema}\n Sometimes, a customer sends information on more than one freight shipment. In this case, make a json object for each shipment and put them all into an array.",
                            "messages": [
                                {
                                    "role": "user",
                                    "content": [{"type": "text", "text": f"Could you parse out the following freight quote? Please be sure to use proper json formatting. Please ignore any information in email signatures from people who work at Circle Logistics, Inc..\n{emailObject['body']}"}]
                                }
                            ]
                        }
                    ),
                )
            
        result = json.loads(response.get("body").read())
        input_tokens = result["usage"]["input_tokens"]
        output_tokens = result["usage"]['output_tokens']
        output_list = result.get("content", [])
                
        print("Invocation details:")
        print(f"- The input length is {input_tokens} tokens.")
        print(f"- The output length is {output_tokens} tokens.")
        print(f"- The model returned {len(output_list)} response(s):")
            
        try:
            leftBracket = output_list[0]['text'].find("{")
            rightBracket = output_list[0]['text'].rfind("}")
                
            object = json.loads(output_list[0]['text'][leftBracket:rightBracket+1])
        except:
            leftBracket = output_list[0]['text'].find("[")
            rightBracket = output_list[0]['text'].rfind("]")
                
            object = json.loads(output_list[0]['text'][leftBracket:rightBracket+1])
            
        if type(object) is not list:
            object = [object]
    
    return object