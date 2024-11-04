import boto3
import os
import datetime
import math
from pymongo import MongoClient
from email_parsing import getEmail
from customer_lookup import customerLookup
from ai_parsing import ai_parsing
from quote_lanes import quote_lanes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

ses = boto3.client('ses',region_name="us-east-1")
CONNECTION_STRING = os.environ['CONNECTION_STRING']
client = MongoClient(host=CONNECTION_STRING)
connect = client.CircleConnect

def post_quote(restructuredObjects, emailObject):
    
    for object in restructuredObjects:
        
        try:
            tproId = int(object['emailData']['tproId'])
        except:
            tproId = None
        
        quote_object = {
            "datHigh": object['rateData']['metadata']['per_trip_high'],
            "datMid": object['rateData']['metadata']['per_trip_rate'],
            "datLow": object['rateData']['metadata']['per_trip_low'],
            "datFuel": object['rateData']['metadata']['per_trip_fuel'],
            "datRecords": None,
            "datCompanies": None,
            "internalHigh": None,
            "internalMid": None,
            "internalLow": None,
            "internalRecords": None,
            "internalCarriers": None,
            "mileage": object['miles'],            
            "DATmileage": None,
            "GoogleMileage": object['miles'],
            "i7customers": None,
            "i7carriers": None,
            "i7records": None,
            "i7high": None,
            "i7mid": None,
            "i7low": None,
            "i7rpm": None,
            "i15customers": None,
            "i15carriers": None,
            "i15records": None,
            "i15high": None,
            "i15mid": None,
            "i15low": None,
            "i15rpm": None,
            "i30customers": None,
            "i30carriers": None,
            "i30records": None,
            "i30high": None,
            "i30mid": None,
            "i30low": None,
            "i30rpm": None,
            "OriginCity": object['origin']['city'].upper(),
            "OriginState": object['origin']['state'],
            "OriginZip": object['origin']['postal_code'],
            "OriginLat": object['origin']['GeoJSON']['coordinates'][1],
            "OriginLong": object['origin']['GeoJSON']['coordinates'][0],
            "DestinationCity": object['destination']['city'].upper(),
            "DestinationState": object['destination']['state'],
            "DestinationZip": object['destination']['postal_code'],
            "DestinationLat": object['destination']['GeoJSON']['coordinates'][1],
            "DestinationLong": object['destination']['GeoJSON']['coordinates'][0],
            "recentLoads": None,
            "truckstopPosted": None,
            "truckstopBookedTrendline": None,
            "datTrendline": None,
            "equipmentType": object['equipment'],
            "pickupDate": datetime.datetime.strptime(object['origin']['start'], "%Y-%m-%d %H:%M:%S"),
            "dateCreated": object['dateCreated'],
            "dateLastModified": object['dateCreated'],
            "createdBy": object['emailData']['accountManager'],
            "status": "Pending",
            "customerId": tproId,
            "customerName": object['emailData']['customerCompany'],
            "baseRateType": "DAT",
            "baseRateValue": object['rateData']['rate'] + object['rateData']['fuel'],
            "marginType": "Margin %",
            "marginValue": ((object['rateData']['calculated_bid'] - (object['rateData']['rate'] + object['rateData']['fuel'])) / object['rateData']['calculated_bid']) * 100,
            "finalRateType": "Flat w/ fuel",
            "fuelSurchargeType": "Per Mile",
            "fuelSurcharge": 0,
            "totalValue": f"{object['rateData']['calculated_bid']:.2f}",
            "finalRate": f"{object['rateData']['calculated_bid']:.2f}",
            "totalLineItems": None,
            "lineItems": None,
            "stopoff": None,
            "postingId": None,
            "origin": {
                "GeoJSON": object['origin']['GeoJSON'],
            },
            "destination": {
                "GeoJSON": object['destination']['GeoJSON'],
            },
            "source": "emailQuotes"
        }
        
        connect['quotes'].insert_one(quote_object)
    
    return    
    
def send_email(restructuredObjects, emailObject):
    
    responder_email = "emailquotes@emaildomain.com"
    message_id = emailObject['messageID']
    subject = emailObject['subject']
    
    
    if emailObject['customerCompany'] == 'CUSTOMER_2':
        
        for load in restructuredObjects:
            if 'calculated_bid' in load['rateData']:
                lane = f"{load['origin']['city']}, {load['origin']['state']} to {load['destination']['city']}, {load['destination']['state']}"
                trailer_requirements = load['actualEquipment']
                all_in = round(load['rateData']['calculated_bid'],2)
                fuel = round(load['miles'] * 0.30, 2)
                linehaul = round(all_in - fuel)
                weight = "45000 pounds"
                duration = load['bid_duration']
                request_number = load['request_number']
                
                plain_text = f"Lane: {lane}\nTrailer Requirements: {trailer_requirements}\nFreight Rate Request #: {request_number}\nAll-in Rate: ${all_in}\n Line-haul Rate: ${linehaul}\nFuel: ${fuel}\nBid duration: {duration}\nMax Capacity Per Truck: {weight}\n"
                html_text = f"""
                        <table cellspacing=3D"0" cellpadding=3D"0" dir=3D"ltr" border=3D"1" style=3D"table-layout:fixed;font-size:10pt;font-family:Arial;width:0px;border-collapse:collapse;border:none">
                            <colgroup>
                                <col width=3D"169">
                                <col width=3D"403">
                            </colgroup>
                            <tbody>
                                <tr style=3D"height:20px">
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;font-weight:bold;border:1px solid rgb(204,204,204)">Lane:</td>
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;border:1px solid rgb(204,204,204)">{lane}</td>
                                </tr
                                <tr style=3D"height:20px">
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;font-weight:bold;border:1px solid rgb(204,204,204)">Trailer Requirements:</td>
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;border:1px solid rgb(204,204,204)">{trailer_requirements}</td>
                                </tr>
                                <tr style=3D"height:20px">
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;font-weight:bold;border:1px solid rgb(204,204,204)">Freight Rate Request #:</td>
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;border:1px solid rgb(204,204,204)">{request_number}</td>
                                </tr>
                                <tr style=3D"height:20px">
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;font-weight:bold;border:1px solid rgb(204,204,204)">All-in Rate:</td>
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;border:1px solid rgb(204,204,204)">${all_in}</td>
                                </tr>
                                <tr style=3D"height:20px">
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;font-weight:bold;border:1px solid rgb(204,204,204)">Line-haul rate:</td>
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;border:1px solid rgb(204,204,204)">${linehaul}</td>
                                </tr>
                                <tr style=3D"height:20px">
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;font-weight:bold;border:1px solid rgb(204,204,204)">Fuel:</td>
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;border:1px solid rgb(204,204,204)">${fuel}</td>
                                </tr>
                                <tr style=3D"height:20px">
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;font-weight:bold;border:1px solid rgb(204,204,204)">Bid duration:</td>
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;border:1px solid rgb(204,204,204)">{duration}</td>
                                </tr>
                                <tr style=3D"height:20px">
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;font-weight:bold;border:1px solid rgb(204,204,204)">Max Capacity Per Truck</td>
                                    <td style=3D"overflow:hidden;padding:0px 3px;vertical-align:bottom;border:1px solid rgb(204,204,204)">{weight}</td>
                                </tr>
                            </tbody>
                        </table>>"""
    
    else:
    
        if emailObject['customerCompany'] == "CUSTOMER_3":
            plain_text = "Whats up brother, we can do:"
            html_text = "<p>Whats up brother, we can do:"
        else:
            plain_text = "Hello, we can do:"
            html_text = "<p>Hello, we can do:"
        
        for load in restructuredObjects:
            
            if 'calculated_bid' in load['rateData']:
                plain_text = plain_text + f"{load['origin']['city']}, {load['origin']['state']}  to  {load['destination']['city']}, {load['destination']['state']} | {load['multistops']} stop off(s) | {load['actualEquipment']} for ${math.ceil(load['rateData']['calculated_bid'])}"
                html_text = html_text + f"<br>{load['origin']['city']}, {load['origin']['state']}  to  {load['destination']['city']}, {load['destination']['state']} | {load['multistops']} stop off(s) | {load['actualEquipment']} for ${math.ceil(load['rateData']['calculated_bid'])}"
    
        html_text = html_text + "</p><p>Thanks.</p>"
    charset = "UTF-8"
    
    new_msg = MIMEMultipart('mixed')
    new_msg['Subject'] = "Re: " + subject
    new_msg['From'] = responder_email
    
    if emailObject['forwarded']:
        
        recipient = emailObject['senderEmail']
    
    else:
        
        recipient = emailObject['accountManager']
    
    new_msg['To'] = recipient
    new_msg['References'] = message_id
    
    msg_body = MIMEMultipart('alternative')
    
    textpart = MIMEText(plain_text.encode(charset), 'plain', charset)
    htmlpart = MIMEText(html_text.encode(charset), 'html', charset)
    
    msg_body.attach(textpart)
    msg_body.attach(htmlpart)
    
    new_msg.attach(msg_body)
    
    if emailObject['customerCompany'] == "CUSTOMER_3":
    
        response = ses.send_raw_email(
            Source=responder_email,
            Destinations=[
                emailObject['accountManager'],
                emailObject['clientStrategyManager']
            ],
            RawMessage={
                'Data':  new_msg.as_string()
            }
        )
        
    else:
        
        response = ses.send_raw_email(
            Source=responder_email,
            Destinations=[
                emailObject['accountManager'],
                emailObject['clientStrategyManager']
            ],
            RawMessage={
                'Data':  new_msg.as_string()
            }
        )
    
    post_quote(restructuredObjects, emailObject)
    
    return
