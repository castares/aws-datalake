import boto3
import json
import logging
import pandas as pd
import os
import re

target_bucket = os.environ['TARGET_BUCKET']
email_source = os.environ["EMAIL_SOURCE"]
email_destination = os.environ["EMAIL_DESTINATION"]


def confirmation_email(subject, body, source, destination):
    ses = boto3.client("ses")

    # Email structure
    message = {
        "Subject": {
            "Data": subject
        },
        "Body": {
            "Html": {
                "Data": body

            }
        }
    }

    # Email sent
    ses.send_email(
        Source=source,
        Destination={
            "ToAddresses": [
                destination
            ]

        },
        Message=message
    )


def count_values_in_range(series, range_min, range_max):
    return series.between(left=range_min, right=range_max).sum()


def lambda_handler(event, context):
    """Read file from s3 on trigger."""
    s3 = boto3.client("s3")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Iterate over the files uploaded in the event.
    for file in event["Records"]:

        # Get name and bucket of the upload
        action = file.get('eventName')
        bucket = str(file.get('s3', {}).get('bucket', {}).get('name'))
        key = str(file.get('s3', {}).get('object', {}).get('key'))

        # Get the file and count number of lines
        file_obj = s3.get_object(Bucket=bucket, Key=key)
        csv_file = file_obj["Body"]
        df = pd.read_csv(csv_file)
        number_of_lines, _ = df.shape
        
        #Standard body for the email.
        email_body = f"""
        Email notification regarding {action} event.<br>
        Bucket : {bucket}<br>
        File Key: {key}.<br>
        Number of lines: {number_of_lines}.<br>
        """

        try:
            # Check key input structure
            device, traffic_type, attack, filename = key.split("/")

            # List of objects in the bucket:
            objects = s3.list_objects(
                Bucket=bucket)
            items = [item.get("Key").split("/")[0] for item in objects.get("Contents", {})]

            # Check for new devices and define logging and email body subsequently:
            if items.count(device) == 1:
                logger.info(
                    f'Event : file uploaded; key : {key}; Number of lines : {number_of_lines}. New Device: {device}'
                )
                new_device = f"New Device: {device}."
                email_body = email_body + new_device

                try: 
                    glue = boto3.client('glue', region_name='us-east-1')
                    response = glue.start_crawler(Name='baiot-input-data')
                    logger.info(
                        response
                    )
                
                except BaseException as error:
                        logger.error(
                            error
                        )

            else:
                logger.info(
                    f'Event : file uploaded; key : {key}; Number of lines : {number_of_lines}')

            # Data Quality: Count the number of values between 0 and 1 on each variable and store the result into a different S3 bucket.
            zero_one = df.T.apply(
                func=lambda row: count_values_in_range(row, range_min=0, range_max=1), axis=1)
            zero_one = zero_one.to_frame().T
            zero_one['device'] = device
            zero_one['traffic_type'] = traffic_type
            zero_one['attack'] = attack
            zero_one.to_csv(f"s3://{target_bucket}/zero_to_one/{device}_{traffic_type}_{filename}", index=False)

        except BaseException as error:
            logger.error(
                error
            )
            warning = f"WARNING!: error appeared processing the file: {error}"
            email_body = email_body + warning

        # Define subject and send email.
        email_subject = f'{action}. Event from bucket: {bucket}'
        confirmation_email(email_subject, email_body,
                           email_source, email_destination)
