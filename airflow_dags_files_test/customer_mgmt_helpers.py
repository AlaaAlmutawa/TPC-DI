from google.cloud import storage
import os
import json
import xmltodict
from constants import SF

converted_file = 'dags/tpc-di_resources/gcs/data/transform/CustomerMgmt.json'
raw_file = 'dags/tpc-di_resources/gcs/data/CustomerMgmt.xml'
gcs_bucket_path_xml = f'{SF}/Batch1/CustomerMgmt.xml'
gcs_bucket = 'tpc_di_staging_files'
path_to_private_key = 'dags/key/tpc-di-370720-38df39751158.json'


def get_customer_mgmt_xml(): 
    client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
    bucket = storage.Bucket(client, gcs_bucket)
    blob = bucket.blob(gcs_bucket_path_xml)
    blob.download_to_filename(raw_file)

def push_customer_mgmt_json(): 
    client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
    bucket = storage.Bucket(client, gcs_bucket)
    blob = bucket.blob(gcs_bucket_path_xml.replace('.xml','.json'))
    blob.upload_from_filename(converted_file)

def convert_xml_to_json():
    with open(raw_file, "r") as file, open(converted_file,
                                                                            "w+") as destination:
        xmltodict.parse(file.read(), item_depth=2, item_callback=append_as_json(destination), attr_prefix="attr_")


def append_as_json(file_handle):
    def add_to_file(path, item):
        print(path)
        action_element = path[1]
        action_attributes = action_element[1]
        item["Customer"]["ActionType"] = action_attributes["ActionType"]
        item["Customer"]["ActionTS"] = action_attributes["ActionTS"]
        file_handle.write(json.dumps(item) + "\n")
        return True

    return add_to_file


