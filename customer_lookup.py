import os
from pymongo import MongoClient

CONNECTION_STRING = os.environ['CONNECTION_STRING']
CUSTOMER_1_KEY = os.environ['CUSTOMER_1_KEY']
CUSTOMER_1_TOKEN = os.environ['CUSTOMER_1_TOKEN']
CUSTOMER_2_KEY = os.environ['CUSTOMER_2_KEY']
CUSTOMER_2_TOKEN = os.environ['CUSTOMER_2_TOKEN']
CUSTOMER_3_KEY = os.environ['CUSTOMER_3_KEY']
CUSTOMER_3_TOKEN = os.environ['CUSTOMER_3_TOKEN']

client = MongoClient(host=CONNECTION_STRING)
loadDb = client.LoadDetail

def grabCustomerKeys(response):
    """ This function takes the response from the customer lookup function and
    resolves the company name to a proper api key and secret token. This is how
    it determines which bot logic to use.
    """
    
    if (response['name'] == "CUSTOMER_2"):
        key = CUSTOMER_1_KEY
        token = CUSTOMER_2_TOKEN
    elif (response['name'] == 'CUSTOMER_3'):
        key = CUSTOMER_1_KEY
        token = CUSTOMER_3_TOKEN
    else:
        key = CUSTOMER_1_KEY
        token = CUSTOMER_1_TOKEN
        
    return key, token
    
def customerLookup(customerEmail):
    """ This function takes the object returned from the email parsing function
    and looks up the email of the sender in the EmailQuotesCustomerLookup 
    collection in MongoDB. In order for a quote to be generated, the email
    must be a verified email in the collection, otherwise it returns None
    """
    
    response = loadDb.EmailQuotesCustomerLookup.find_one({
        "verifiedEmails": { "$in": [customerEmail] }
    },
    {
        "name": 1,
        "tproId": 1,
        "accountManager": 1,
        "clientStrategyManager": 1,
        "defaultEquipment": 1
    })
    
    if response:
        key, token = grabCustomerKeys(response)
    
    else:
        return None, None, None

    
    return key, token, response
        