import json
import sys
from confluent_kafka import Consumer, KafkaException
import boto3
import os
from pathlib import Path
import uuid
import config_reader
import json
 
 
with open('/home/config.json', 'r', encoding='utf-8') as file:
        conf = json.load(file)
 
 
def upload_to_s3(bucket_name, source_path,data):
    """
    Uploads files to an S3 bucket.
 
    Args:
        bucket_name (str): The name of the S3 bucket.
        source_path (str): The path to the file or folder to be uploaded.
 
    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    try:
        # Create an S3 client
        s3 = boto3.resource('s3',
                             aws_access_key_id=conf.get('general_s3').get('aws_access_key_id'),
                             aws_secret_access_key=conf.get('general_s3').get('aws_secret_access_key'))
        # Check if the source path is a file or a folder
        if os.path.isfile(source_path):
            # If it's a file, upload the file
            file_name = os.path.basename(source_path)
            with open(source_path, 'rb'):
                s3.Bucket(bucket_name).put_object(Key='group4/'+file_name+str(uuid.uuid4()), Body=str(data))
            print(f'File {source_path} uploaded to S3 bucket {bucket_name}')
       
        elif os.path.isdir(source_path):
           
            # If it's a folder, upload all files in the folder
            for root, _, files in os.walk(source_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, source_path)
                    with open(file_path, 'rb') as file_data:
                        s3.upload_file(file_data, bucket_name, relative_path)
                    print(f'File {file_path} uploaded to S3 bucket {bucket_name}')
        else:
            print(f'Invalid source path: {source_path}')
            return False
 
        return True
    except Exception as e:
        print(f'Error uploading files to S3 bucket {bucket_name}: {e}')
        return False
 
 
def delete_s3_object(bucket_name, object_name):
    """
    Deletes an object from an S3 bucket.
 
    Args:
        bucket_name (str): The name of the S3 bucket.
        object_name (str): The name (key) of the object to be deleted.
 
    Returns:
        bool: True if the object was deleted successfully, False otherwise.
    """
    try:
        # Create an S3 resource
        s3 = boto3.resource('s3', aws_access_key_id=conf.get('general_s3', 'aws_access_key_id'),
                             aws_secret_access_key=conf.get('general_s3', 'aws_secret_access_key'))
        # Get the bucket object
        bucket = s3.Bucket(bucket_name)
 
        #Check if the object exists in the bucket
        objs = list(bucket.objects.filter(Prefix=object_name))
        if not objs:
            print(f'Object {object_name} does not exist in bucket {bucket_name}')
            return False
 
        # Get the object and delete it
        obj = bucket.Object(object_name)
        obj.delete()
 
        print(f'Object {object_name} deleted from bucket {bucket_name}')
        return True
   
    except Exception as e:
        print(f'Error deleting object {object_name} from bucket {bucket_name}: {e}')
        return False
   
 
 
'''def main():
    bucket_name = 'devops-training-504956989193'
    object_name = "group6/WhatsApp Image 2026-03-10 at 10.23.52.jpeg"
    upload_to_s3(bucket_name=bucket_name,source_path="/home/mtmn13/MatmonCoin/docker-compose.yaml")
    # if delete_s3_object(bucket_name, object_name):
    #     print("succ")
    # else:
    #     print("failed")'''
   
def write_to_s3(msg):
    print("**************************")
 
    bucket_name = 'devops-training-504956989193'
    current_file_dir = Path(__file__).resolve().parent
    path = str(current_file_dir)+"\\try\\" + str(uuid.uuid4())+".txt"
    print(path)
    try:
        with open(path, "w") as text_file:
            text_file.write(str(msg))
        upload_to_s3(bucket_name=bucket_name,source_path=str(current_file_dir)+"try.txt",data=msg)
    except:
        print("didnt work")
    finally:
        os.remove(path)
def first_time():
    current_file_dir = Path(__file__).resolve().parent
    path = str(current_file_dir)+"try.txt"
    print(path)
    with open(path, "w") as text_file:
        text_file.write("")
   
   
   
 
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 's3-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}
 
consumer = Consumer(consumer_conf)
first_time()
 
topic = 'store-first-topic'
consumer.subscribe([topic])
 
 
try:
    while True:
        msg = consumer.poll(1.0)
        print("************")
        if msg is None:
            print("NONE")
            continue
           
        if msg.error():
            print(msg.error())
        else:
 
            new_msg = json.loads(msg.value().decode('utf-8'))
            write_to_s3(new_msg)
            consumer.commit()
 
except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')
finally:
    consumer.close()
 
 