import requests
from dataclasses import dataclass
import json
from typing import List, Dict
import time


class SerializeHubspotResponse:
    """This class is used to serialize the json response from hubspot to List[Dict] format"""

    @staticmethod
    def serialize_summary(hubspot_json_response:Dict):
        serialized_json = []
        
        for date,data in hubspot_json_response.items():
            for record in data:
                record.update({"date":date})
                serialized_json.append(record)
        
        return serialized_json
    
    @staticmethod
    def serialize_totals(hubspot_json_response:Dict,date:str):
        serialized_json = hubspot_json_response['breakdowns']
        for record in serialized_json:
            record.update({"date":date})
        
        return serialized_json

    @staticmethod
    def add_key_value(serialized_hubspot_response:list,data:dict):
        for record in serialized_hubspot_response:
            record.update(data)
        return serialized_hubspot_response


@dataclass
class HubSpotApiCredentials:
    """This class taken all required parameter to call hubspot Api"""
    base_url:str
    endpoint:str
    token:str
        
class HubSpotSourceApi:
    """This class is used to call Hubspot traffic by source api at level 1 without any further drill down"""
    
    @staticmethod
    def get_daily(credentials:HubSpotApiCredentials,start_date:str,end_date:str,drill_down_value1:str=None,
            drill_down_value2:str=None,filter:str=None,limit:int=None,offset:int=None):
        params = {
            "start":start_date,
            "end":end_date,
            "d1":drill_down_value1,
            "d2":drill_down_value2,
            "f":filter,
            "limit":limit,
            "offset":offset

        }
        headers = {
            "Authorization" : f"Bearer {credentials.token}"
        }
        response = requests.get(
                                url=f"{credentials.base_url}/{credentials.endpoint}",
                                params=params,
                                headers=headers
                            )
        if response.status_code==200:
            data = json.loads(response.text)
            return data
        else:
            return {"errorCode":response.status_code,"errorDescription":response.text}
    
    @staticmethod
    def get_total(credentials:HubSpotApiCredentials,date:str,drill_down_value1:str=None,
            drill_down_value2:str=None,filter:str=None,limit:int=None,offset:int=None):
        
        params = {
            "start":date,
            "end":date,
            "d1":drill_down_value1,
            "d2":drill_down_value2,
            "limit":limit,
            "offset":offset
        }
        headers = {
            "Authorization" : f"Bearer {credentials.token}"
        }
        response = requests.get(
                                url=f"{credentials.base_url}/{credentials.endpoint}",
                                params=params,
                                headers=headers
                            )
        if response.status_code==200:
            data = json.loads(response.text)
            # response_headers = {"x-hubspot-ratelimit-daily":response.headers.get("x-hubspot-ratelimit-daily"),
            #                     "X-HubSpot-RateLimit-Daily-Remaining":response.headers.get("X-HubSpot-RateLimit-Daily-Remaining"),
            #                     "X-HubSpot-RateLimit-Interval-Milliseconds":response.headers.get("X-HubSpot-RateLimit-Interval-Milliseconds"),
            #                     "X-HubSpot-RateLimit-Max":response.headers.get("X-HubSpot-RateLimit-Max"),
            #                     "X-HubSpot-RateLimit-Remaining":response.headers.get("X-HubSpot-RateLimit-Remaining")}
            
            # return {"data":data,"headers":response_headers}
            return data
        else:
            return {"errorCode":response.status_code,"errorDescription":response.text}
        

def get_hubspot_source_key(hubspot_json_response:dict) -> List:

    key_collection = []

    serialized_data:List[Dict] = SerializeHubspotResponse.serialize(hubspot_json_response)

    for record in serialized_data:
        for key,value in record.items():
            if key not in key_collection:
                key_collection.append(key)

    return key_collection

# def check_api_rate_limit(**kwargs):
#     def inner(hubspot_api_func):
#         if len(kwargs.items()) > 0:
#             if kwargs['X-HubSpot-RateLimit-Remaining'] > 0 and kwargs['X-HubSpot-RateLimit-Daily-Remaining'] > 0:
#                 return hubspot_api_func
#         else:
#             time.sleep(10)
#             return hubspot_api_func
#     return inner