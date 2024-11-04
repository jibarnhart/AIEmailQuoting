import boto3
from pymongo import MongoClient

lcs = boto3.client('location')

state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
    "Ontario": "ON",
    "Quebec": "QC",
    "Nova Scotia": "NS",
    "New Brunswick": "NB",
    "Manitoba": "MB",
    "British Columbia": "BC",
    "Prince Edward Island": "PE",
    "Saskatchewan": "SK",
    "Alberta": "AB",
    "Newfoundland and Labrador": "NL",
    "Aguascalientes": "AG",
    "Baja California": "BJ",
    "Baja California Sur": "BS",
    "Campeche": "CP",
    "Chihuahua": "CI",
    "Coahuila": "CU",
    "Colima": "CL", 
    "Distrito Federal": "DF",
    "Durango": "DG",
    "Guanajuato": "GT",
    "Guerrero": "GR",
    "Hidalgo": "HG",
    "Jalisco": "JA",
    "Mexico": "EM",
    "Michoacan": "MH",
    "Morelos": "MR",
    "Nayarit": "NA",
    "Oaxaca": "OA",
    "Puebla": "PU",
    "Queretaro": "QT",
    "Quintana Roo": "QR",
    "San Luis Potosi": "SL",
    "Sinaloa": "SI",
    "Sonora": "SO",
    "Tabasco": "TA",
    "Tamaulipas": "TM",
    "Tlaxcala": "TL",
    "Veracruz": "VE",
    "Yucatan": "YC",
    "Zacatecas": "ZT",
    "Nuevo Leon": "NN",
    "Chiapas": "CS"
}

def geocode(string):
    """This function takes a location string and returns a location data object.
    """
    
    response = lcs.search_place_index_for_text(
        FilterCountries=['USA','CAN','MEX'],
        Language="en",
        MaxResults=1,
        Text=string,
        IndexName="email-quoting-place-index"
    )['Results'][0]
    
    location = {
        "city": response['Place']['Municipality'],
        "state": state_to_abbrev[response['Place']['Region']],
        "postal_code": response['Place']['PostalCode'],
        "country": response['Place']['Country'],
        "placeId": response['PlaceId'],
        "GeoJSON": {
            "type": "Point",
            "coordinates": response['Place']['Geometry']['Point']
        },
        "timezone": response['Place']['TimeZone']['Name']
    }
    
    return location
        
def routeCalculating(object):
    """ This function calculates the route between the load's origin and 
    destination. It returns the mileage, the route geometry and the bounding box
    for the map.
    """
    
    if 'stops' in object:
        
        waypoints = []
        for stop in object['stops']:
            waypoints.append(stop['GeoJSON']['coordinates'])
            
        response = lcs.calculate_route(
            CalculatorName='email-quoting-calculator',
            DeparturePosition=object['origin']['GeoJSON']['coordinates'],
            DestinationPosition=object['destination']['GeoJSON']['coordinates'],
            DistanceUnit='Miles',
            TravelMode='Truck',
            IncludeLegGeometry=True,
            WaypointPositions = waypoints
        )
        
        totalGeometry = []
        
        for leg in response['Legs']:
            totalGeometry = totalGeometry + leg['Geometry']['LineString']
        
        routeData = {
            'distance': response['Summary']['Distance'],
            'geometry': {
                'lineString': totalGeometry
            },
            'bbox': response['Summary']['RouteBBox']
        }
        
    else:
    
        response = lcs.calculate_route(
            CalculatorName='email-quoting-calculator',
            DeparturePosition=object['origin']['GeoJSON']['coordinates'],
            DestinationPosition=object['destination']['GeoJSON']['coordinates'],
            DistanceUnit='Miles',
            TravelMode='Truck',
            IncludeLegGeometry=True
        )
    
        routeData = {
            'distance': response['Summary']['Distance'],
            'geometry': {
                'lineString': response['Legs'][0]['Geometry']['LineString']
            },
            'bbox': response['Summary']['RouteBBox']
        }
    
    return routeData
    
def location_services_handler(restructuredObject):
    """ The handler function for calling the location services functions.
    """
    
    route = routeCalculating(restructuredObject)
    
    restructuredObject['routingData'] = route
    
    return restructuredObject
    
    
