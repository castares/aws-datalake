import boto3
import uuid

s3 = boto3.resource('s3')

def create_bucket(bucket_prefix):
    try: 
        bucket_name = f'{bucket_prefix}-{uuid.uuid4()}'
        bucket = s3.create_bucket(Bucket=bucket_name)
        print(f'Bucket with name {bucket.name} created.')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    prefix = input("Enter bucket prefix: ")
    create_bucket(prefix)