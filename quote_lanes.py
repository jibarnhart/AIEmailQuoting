import json
import boto3
import os
import requests
import datetime
import math
from pymongo import MongoClient
from location_services import location_services_handler, geocode

CONNECTION_STRING = os.environ['CONNECTION_STRING']
PRICING_URL = os.environ['PRICING_URL']
ENDPOINT = os.environ['ENDPOINT']
API_KEY = os.environ['API_KEY']

headers = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json"
}

s3 = boto3.client('s3')
brt = boto3.client(service_name='bedrock-runtime')
lambda_client = boto3.client('lambda')

client = MongoClient(host=CONNECTION_STRING)
loadDb = client.LoadDetail

def getEquipment(equipment, endpoint=ENDPOINT,headers=headers):
    """ This uses the API that Jacob put together to return either VAN, REEFER,
    or FLATBED from the equipment type parsed out from the email.
    """
    try:
        response = requests.get(ENDPOINT,  json=[equipment], headers=headers)
        return response.json()
    except:
        response = requests.get(ENDPOINT,  json=["VAN"], headers=headers)
        return response.json()
    
def validateEquipment(load, emailObject):
    """ Simple function for ensuring every load has an equipment type. For some
    companies we will be able to assume a certain equipment type if they did
    not specify. Others, we will need to do another process.
    """
    
    if (load['equipmentType'] == "" or load['equipmentType'] is None):
        equipment = emailObject['defaultEquipment']
        return equipment
    else:
        formattedEquipment = getEquipment(load['equipmentType'])
        return(formattedEquipment[load['equipmentType']])
        
def getBotRate(laneData, key, token):
    """ Calls the pricing engine 
    """

    pricingHeaders = {
        "x-api-key": key, 
        "secret-token": token
    }
    
    response = requests.post(PRICING_URL, headers=pricingHeaders, json=laneData)
    response = json.loads(response.text)
    
    remove = []
    
    for key in response:
        if key in laneData:
            remove.append(key)
    
    for key in remove:
        del response[key]
        
    rateData = response
    
    return rateData

def getInternalRate(object): 
    """ Calls the internal rate lambda function
    """
    request_body = {
        "body": {
            "originLatitude": object['origin']['GeoJSON']['coordinates'][1],
            "originLongitude": object['origin']['GeoJSON']['coordinates'][0],
            "destinationLatitude": object['destination']['GeoJSON']['coordinates'][1] ,
            "destinationLongitude": object['destination']['GeoJSON']['coordinates'][0],
            "originLocationType": 'coordinates',
            "destinationLocationType": 'coordinates',
            "originRadius": 50,
            "destinationRadius": 50,
            "equipmentCategory": object['equipment'],
            "reportType": "summary",
            "terminals": [102, 106, 274, 267, 130, 126, 278, 125, 127, 129, 105, 122, 268, 124, 123, 285, 284]
        }
    }

    invoke_response = lambda_client.invoke(FunctionName="lane-similarity-one",
                        InvocationType='RequestResponse',
                        Payload=json.dumps(request_body)
                    )
    try:
         response = json.loads(invoke_response['Payload'].read())['body']['summary30Day']
    except:
        response = None
    
    return response

def validateDates(load, restructuredObject):
    """ Function for ensuring we always have dates. If there are pickup and
    delivery dates, we just convert the date strings to datetimes. If there is
    not a pickup date, we assume today. If there is not a delivery date we take
    the mileage and divide by 600 (or 1200 if a team is required) and round up 
    to the nearest whole number to get the approximate number of days required 
    to move the load and add that number of days to the pickup date.
    """
    
    if load['pickupDate'] and load['pickupDate'] != "":
        
       load['pickupDate'] = datetime.datetime.strptime(load['pickupDate'], "%Y-%m-%d")
       pickupDateInferred = False
     
    else:
         
         load['pickupDate'] = datetime.datetime.now()
         pickupDateInferred = True
    
    if load['deliveryDate'] and load['deliveryDate'] != "":
        
        load['deliveryDate'] = datetime.datetime.strptime(load['deliveryDate'], "%Y-%m-%d")
        deliveryDateInferred = False
        
    else:
        
        if load['teamRequired']:
            
            daysToDeliver = math.ceil(restructuredObject['routingData']['distance'] / 1200)
        
        else:
            
            daysToDeliver = math.ceil(restructuredObject['routingData']['distance'] / 600)
    
        load['deliveryDate'] = load['pickupDate'] + datetime.timedelta(days=daysToDeliver)
        deliveryDateInferred = True
        
    restructuredObject['origin']['start'] = load['pickupDate'].strftime("%Y-%m-%d %H:%M:%S")
    restructuredObject['origin']['end'] = load['pickupDate'].strftime("%Y-%m-%d %H:%M:%S")
    restructuredObject['destination']['start'] = load['deliveryDate'].strftime("%Y-%m-%d %H:%M:%S")
    restructuredObject['destination']['end'] = load['deliveryDate'].strftime("%Y-%m-%d %H:%M:%S")
    
    return restructuredObject

def quote_lanes(object, emailObject, key, token):
    """ The handler function that loops through each load needing to be quoted.
    At the end we insert all loads into mongo.
    """
    
    objects = []
    

    for load in object:
        
        actualEquipment = load['equipmentType']
        load['equipmentType'] = validateEquipment(load, emailObject)
        
        number_of_stops = len(load['stops'])
        
        
        stops = []
        for stop in load['stops']:
            location = geocode(stop)
            stops.append(location)
            
        if (emailObject['customerCompany'] == "Bimbo Bakehouse North America"):
            load['equipmentType'] = "REEFER"
        elif (emailObject['customerCompany'] == "Beacon Roofing Supply, Inc. c/o Cass Information Systems"):
            load['equipmentType'] = "FLATBED"
            
        if emailObject['customerCompany'] == "EP Minerals / US Silica":
            restructuredObject = {
                "origin": stops[0],
                "destination": stops[-1],
                "stops": stops[1:-1],
                "multistops": number_of_stops-2,
                "weight": load['weight'],
                "shipment_type": "FTL",
                "is_hazmat": eval(load['isHazmat']),
                "equipment": load['equipmentType'],
                "actualEquipment": actualEquipment,
                "tanker_endorsement": eval(load['tankerEndorsement']),
                "team_required": eval(load['teamRequired']),
                "extra_information": load['importantNotes'],
                "pieces": load['pieces'],
                "request_number": load['request_number'],
                "bid_duration": load['bid_duration']
            }
        
        else:
            restructuredObject = {
                "origin": stops[0],
                "destination": stops[-1],
                "stops": stops[1:-1],
                "multistops": number_of_stops-2,
                "weight": load['weight'],
                "shipment_type": "FTL",
                "is_hazmat": eval(load['isHazmat']),
                "equipment": load['equipmentType'],
                "actualEquipment": actualEquipment,
                "tanker_endorsement": eval(load['tankerEndorsement']),
                "team_required": eval(load['teamRequired']),
                "extra_information": load['importantNotes'],
                "pieces": load['pieces']
            }

        if (load['isHazmat'] and load['unNumber'] is not None and load['unNumber'] != "" and load['unNumber'] != "0000"):
            
            un_info = loadDb['UNNumberLookup'].find_one({
                "un_number": load['unNumber']
            },
            {
                "_id": 0
            })
            
            restructuredObject['hazmatInfo'] = un_info
            
        if ('hazmatInfo' in restructuredObject and (restructuredObject['hazmatInfo']['class'] == "1" or restructuredObject['hazmatInfo']['class'] == '7')):
            
            restructuredObject['rateData'] = {
                'did_bid': False,
                'no_bid_reason': f"Class {restructuredObject['hazmatInfo']['class']} hazardous material. Can not bid."
            }
            
        else:    
            restructuredObject = location_services_handler(restructuredObject)
            restructuredObject = validateDates(load, restructuredObject)
            restructuredObject['miles'] = restructuredObject['routingData']['distance']
        
            rateData = getBotRate(restructuredObject, key, token)
            restructuredObject['rateData'] = rateData
            restructuredObject['emailData'] = emailObject
            restructuredObject['internalData'] = getInternalRate(restructuredObject)
            
            if restructuredObject['rateData']['calculated_bid'] < restructuredObject['rateData']['minimum_bid']:
                restructuredObject['rateData']['calculated_bid'] = restructuredObject['rateData']['minimum_bid']
            
            if restructuredObject['internalData'] is not None:
                
                if restructuredObject['miles'] > 250:
                    margin = restructuredObject['rateData']['calculated_bid'] - (restructuredObject['rateData']['rate'] + restructuredObject['rateData']['fuel'])
                    restructuredObject['internalData']['internal_bid'] = (restructuredObject['internalData']['rpm'] * restructuredObject['miles'])  + margin
                    restructuredObject['rateData']['final_bid'] = (restructuredObject['internalData']['internal_bid'] + restructuredObject['rateData']['calculated_bid']) / 2
                else:
                    margin = restructuredObject['rateData']['calculated_bid'] - (restructuredObject['rateData']['rate'] + restructuredObject['rateData']['fuel'])
                    restructuredObject['internalData']['internal_bid'] = restructuredObject['internalData']['avgTransCost'] + margin
                    restructuredObject['rateData']['final_bid'] = (restructuredObject['internalData']['internal_bid'] + restructuredObject['rateData']['calculated_bid']) / 2
            else:
                restructuredObject['rateData']['final_bid'] = restructuredObject['rateData']['calculated_bid']
            
            
        laneString = f"{restructuredObject['origin']['city'].title()}, {restructuredObject['origin']['state'].upper()} -> {restructuredObject['destination']['city'].title()}, {restructuredObject['destination']['state'].upper()}"
        equipmentString = f"{load['shipmentType']} {load['equipmentType']}"
        
        if restructuredObject['is_hazmat']:
            equipmentString = equipmentString + " Hazmat"
                
        if restructuredObject['tanker_endorsement']:
            equipmentString = equipmentString + " Tanker Endorsement Required"
                
        if  restructuredObject['team_required']:
            equipmentString = equipmentString + " Team load"
            
        if 'calculated_bid' in restructuredObject['rateData']:
            print(f"Quote requested from {emailObject['customerCompany']} | {laneString} | {equipmentString} {load['weight']}lbs {load['pieces']} | Bid calculated at ${restructuredObject['rateData']['calculated_bid']}")
            
        restructuredObject['dateCreated'] = datetime.datetime.now()
        restructuredObject['equipmentString'] = equipmentString
            
        restructuredObject['rateData']['rateAccepted'] = None
        
        objects.append(restructuredObject)
        
    loadDb.EmailQuotes.insert_many(objects)
    
    return(objects)
    
    