import boto3
import os

ARN = os.environ['ARN']

sns = boto3.client('sns')

def sns_send(restructuredObjects, emailObject):
    
    subject = f"{emailObject['customerCompany']} has requested a quote."
    message = f"{emailObject['customerCompany']} has requested a quote on the following lane(s):\n"

    for load in restructuredObjects:
        #print(load)
        laneString = f"{load['origin']['city'].title()}, {load['origin']['state']} -> {load['destination']['city'].title()}, {load['destination']['state']}"
        equipmentString = f"{load['shipment_type']} {load['equipment']}"
        message = message + f"{laneString} | {equipmentString} {load['weight']}lbs {load['pieces']} "
        
        if 'stops' in load:
            message = message + f"| {len(load['stops'])} stop off(s) "
            
        if 'calculated_bid' in load['rateData']:
            message = message + f"| Bid calculated at ${round(load['rateData']['calculated_bid'], 2)}\n"
        elif (load['rateData']['no_bid_reason'] is not None and 'Can not bid' in load['rateData']['no_bid_reason']):
            message = message + f"| Can not haul class {load['hazmatInfo']['class']} hazardous materials"
    
    response = sns.publish(
        TopicArn = ARN,
        Message = message,
        Subject = subject
    )
    
    return response
