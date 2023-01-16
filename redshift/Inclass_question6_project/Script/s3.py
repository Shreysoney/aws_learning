import boto3
def create_buc(bucket_name:str):
    ''' Creates a bucket with the bucket-name in S3
        Parameter:
            bucket_name: a string'''

    client = boto3.client('s3')
    try:
        response = client.create_bucket(Bucket=bucket_name)
    except Exception as e:
        print(e)

def check_buc_name(bucket_name:str):
    ''' Checks whether given S3 Bucket is present or not.
        It also display all the list of buckets
        Parameter:
            bucket_name: a string'''

    client = boto3.client('s3')
    response1 = client.list_buckets()
    list_of_buckets=[]
    for data in response1['Buckets']:
        list_of_buckets.append(data['Name'])
    print(list_of_buckets)
    if bucket_name in list_of_buckets:
        print(bucket_name,':Bucket is present')
    else:
        print('No Bucket Found by Name:',bucket_name)


def upload(file_name:str,bucket_name:str,folder_name=None):
    ''' Uploads the given file to the S3 Bucket from Local Machine
        Parameter:
            file_name: a string
            bucket_name: a string
            '''

    s3 = boto3.resource(service_name='s3')
    if folder_name is None:
        folder_name=file_name
    s3.meta.client.upload_file(Filename='C:/Users/Shrey/Downloads/'+f'{file_name}', Bucket=bucket_name, Key=folder_name)

def create_buc_obj(bucket_name:str,folder_name:str):
    ''' Creates a bucket Objects with the file_name in S3
        Parameter:
            bucket_name: a string
            foler_name: a string'''

    client = boto3.client('s3')
    try:
        response = client.put_object(Bucket=bucket_name,Key=folder_name)
    except Exception as e:
        print(e)

#upload('tt.csv','tranformdata','abc/tt.csv')