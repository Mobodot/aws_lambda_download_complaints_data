from pymongo import MongoClient
import requests
import json
from datetime import datetime
import logging 
import boto3
import os


CONNECTION_STRING = os.getenv("CONNECTION_STRING")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
BUCKET_NAME = os.getenv("BUCKET_NAME")

API_DATA_URL = "https://www.consumerfinance.gov/data-research/"\
            "consumer-complaints/search/api/v1/"\
            "?date_received_max=<to-date>"\
            "&date_received_min=<from-date>&"\
            "field=all&format=json"
    


client = MongoClient(CONNECTION_STRING)

def get_api_data(from_date, to_date):
    try:
        url = API_DATA_URL.replace("<from-date>", from_date)\
                            .replace("<to-date>", to_date)
        res = requests.get(url)
        data = list(map(lambda x: x["_source"], 
                filter(lambda x: "_source" in x.keys(),json.loads(res.content))
                    ))
        return data
    except ConnectionError as e:
        print("Unable to fetch the data: Connection Error!")
    except Exception as e:
        print(e)
        


def create_db_and_collection():
    try:
        if not DATABASE_NAME in client.list_database_names():
            db = client[DATABASE_NAME]
            collection = db[COLLECTION_NAME]
            return "DB & collection created successfully!!" 
        
        return "DB already exists"
        
    except Exception as e:
        print(e)
    

def get_from_date_to_date():
    from_date = "2023-02-08"
    
    if COLLECTION_NAME in client[DATABASE_NAME].list_collection_names():
        query = [{
                    "$group": {
                        "_id": None,
                        "max_to_date": {"$max": "$to_date"}
                    }
                }]
        
        record = list(client[DATABASE_NAME][COLLECTION_NAME].aggregate(query))
        if record:
            from_date = record[0]["max_to_date"]
            from_date = from_date.strftime("%Y-%m-%d")
    
    to_date = datetime.now().strftime("%Y-%m-%d")
    
    return {
        "from_date": from_date,
        "to_date": to_date,
        "from_date_obj": datetime.strptime(from_date, "%Y-%m-%d"),
        "to_date_obj": datetime.strptime(to_date, "%Y-%m-%d")
    }


def lambda_handler(event, context):
    print("Creating Database and collection")
    create_db_and_collection()
    
    print("Obtaining recent date from db our setting default: (default: 2023-03-08)")
    from_date,to_date,from_date_obj,to_date_obj = get_from_date_to_date().values()
    # handle the case when we want to collect 1 days' data. i.e data from yesterday till today.
    if from_date == to_date:
        return {
            "status code": 200,
            "body": json.dumps("Pipleline has already downloaded all the data till date!!")
        }
        
    print("Getting api data ...")
    finance_complaint_data = get_api_data(from_date, to_date)
    
    # insert data to mongodb
    record1 = {
        "from_date": from_date_obj,
        "to_date": to_date_obj,
        "compliant_data": finance_complaint_data
    } 
    
    print("Inserting record into Database")
    record = client[DATABASE_NAME][COLLECTION_NAME].insert_one(record1)
    if record.acknowledged:
        logging.info("Record Entered successfully!!!")
    else:
        logging.error("Error occured while inserting data!!!")
        
    # insert data to s3
    print("Preparing s3 objects")
    s3 = boto3.resource("s3")
    s3_object = s3.Object(BUCKET_NAME, 
                          f"{from_date.replace('-','_')}_{to_date.replace('-','_')}_compliant_data")
    
    print(f"Inserting data in bucket: {BUCKET_NAME}")
    s3_object.put(
        Body=bytes(json.dumps(finance_complaint_data).encode("UTF-8")))
    
    # record = save_from_date_to_date(finance_complaint_data)
    
# lambda_handler()


