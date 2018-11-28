from oci.object_storage import UploadManager
from oci.object_storage.transfer.constants import MEBIBYTE
import json
import oci
import sys
import os
import os.path
import io
import ast
from oci.object_storage.models import CreateBucketDetails
import cx_Oracle
from tqdm import tqdm


print("=== checking global parameters ===")
config = oci.config.from_file("config","DEFAULT")
identity = oci.identity.IdentityClient(config)
user = identity.get_user(config["user"]).data
print(user)
compartment_dets = config["compartment"]
bucket_name = '<PUT YOUR BUCKET NAME HERE'
tenancy= '<YOUR TENANCY>'
region = config['region']
print(compartment_dets)
object_storage = oci.object_storage.ObjectStorageClient(config)
namespace = object_storage.get_namespace().data
connection = cx_Oracle.connect('username', 'password#', 'tnsname') 
cursor=connection.cursor();
print(namespace)
print("=== Finsh checking global parameters ===")


def progresscallback(bytes_uploaded):
    print("{} additional bytes uploaded".format(bytes_uploaded))
    identity = oci.identity.IdentityClient(config)
    user = identity.get_user(config["user"]).data
    print(user)
    compartment_dets = config["compartment"]
    print(compartment_dets)


def upload_data_Object_storage ():

    directory = os.path.dirname(os.path.abspath(__file__))
    directory +='\\civil-aviation-authority-of-the-philippines-passenger-movement-data'
    object_storage = oci.object_storage.ObjectStorageClient(config)
    files_to_process = [file for file in os.listdir(directory) if file.endswith('csv')]

    try:
        for upload_file in files_to_process:
            print('Uploading file {}'.format(upload_file))
            print(upload_file)
            partsize = 1000 * MEBIBYTE
            print(partsize)
            object_name=upload_file
            filename=os.path.join(directory,upload_file)
            upload_manager=UploadManager(object_storage,allow_parallel_uploads=True,allow_multipart_uploads=True)
            response=upload_manager.upload_file(namespace,bucket_name,object_name,filename,part_size=partsize,progress_callback=progresscallback)
            print(response.data)


    except Exception as e:
        print(e.message)


def create_token():
    # creating the token
    token = identity.create_auth_token(
        oci.identity.models.CreateAuthTokenDetails(
            description = "Token used to provide access to newly loaded files"
        ),
        user_id = config['user']
    )

    

    cursor.callproc('DBMS_CLOUD.create_credential', keywordParameters = {'credential_name':'UPLOAD_DATA_AUTH',
                                                                    'username':'your username',
                                                                    'password':token.data.token})

    

def upload_data_to_ATP ():
    print("Begin upload")
    bucket = object_storage.get_bucket(namespace, bucket_name)
    object_list = object_storage.list_objects(namespace, bucket_name)

    format = "{'delimiter':',','skipheaders':'1', 'rejectlimit':'1000', 'trimspaces':'rtrim', 'ignoreblanklines':'true', 'ignoremissingcolumns':'true'}"

    for o in object_list.data.objects:
        file_location = "https://swiftobjectstorage.us-ashburn-1.oraclecloud.com/v1/oscjapac002/samplebucket/" + str(o.name)
        print(file_location)

        rs = cursor.execute("select table_name from user_tables where table_name not like 'COPY%'")
        rows = rs.fetchall()
        for row in tqdm(rows):
            url = file_location.format(region=region, tenancy=tenancy, bucket_name=bucket_name, table_name='airdata_passenger_movement_2001')
            cursor.callproc('DBMS_CLOUD.copy_data', keywordParameters= {'table_name':'airdata_passenger_movement_2001',
                                                           'credential_name':'UPLOAD_DATA_AUTH',
                                                           'file_uri_list':url, 
                                                           'format': format
                                                           })
    print("Ended upload")



