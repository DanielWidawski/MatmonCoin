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
 
 # Create an S3  client
s3_client = boto3.resource('s3', aws_access_key_id=conf.get('general_s3', 'aws_access_key_id'),
        aws_secret_access_key=conf.get('general_s3', 'aws_secret_access_key')) 

s3_consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 's3-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

s3_consumer = Consumer(s3_consumer_conf)
topic = 'store-first-topic'
s3_consumer.subscribe([topic])


def upload_to_s3(bucket_name, source_path,data):
    try:
        # Check if the source path is a file or a folder
        if os.path.isfile(source_path):
            # If it's a file, upload the file
            file_name = os.path.basename(source_path)
            with open(source_path, 'rb'):
                s3_client.Bucket(bucket_name).put_object(Key='group4/'+file_name+str(uuid.uuid4()), Body=str(data))
            print(f'File {source_path} uploaded to S3 bucket {bucket_name}')
       
        elif os.path.isdir(source_path):
           
            # If it's a folder, upload all files in the folder
            for root, _, files in os.walk(source_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, source_path)
                    with open(file_path, 'rb') as file_data:
                        s3_client.upload_file(file_data, bucket_name, relative_path)
                    print(f'File {file_path} uploaded to S3 bucket {bucket_name}')
        else:
            print(f'Invalid source path: {source_path}')
            return False
 
        return True
    except Exception as e:
        print(f'Error uploading files to S3 bucket {bucket_name}: {e}')
        return False
 

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


def first_time_write_to_s3():
    current_file_dir = Path(__file__).resolve().parent
    path = str(current_file_dir)+"try.txt"
    print(path)
    with open(path, "w") as text_file:
        text_file.write("")
   
first_time_write_to_s3()

def send_messages_to_s3() -> None: 
    try:
        while True:
            msg = s3_consumer.poll(1.0)
            print("************")
            if msg is None:
                print("NONE")
                continue
            
            if msg.error():
                print(msg.error())
            else:
    
                new_msg = json.loads(msg.value().decode('utf-8'))
                write_to_s3(new_msg)
                s3_consumer.commit()
    
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        s3_consumer.close()
 