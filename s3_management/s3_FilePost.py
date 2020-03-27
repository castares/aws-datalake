#!/usr/bin/env python3

import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import argparse
import os
import sys

load_dotenv()

bucket = os.getenv("TARGET_BUCKET")


def parseArgs():
    # Create the parser
    parser = argparse.ArgumentParser(prog='S3_FilePost',
                                     description='Post a file to S3 using the default account set on AWS CLI')

    # Add the arguments
    parser.add_argument('Path',
                        type=str,
                        help='The path to the file')
    parser.add_argument('Bucket',
                        type=str,
                        help='The destination bucket')
    parser.add_argument('Key',
                        action='store',
                        nargs=4,
                        type=str,
                        help='Object key structure for S3. Must contain 4 arguments: device, traffic_type, attack and filename')

    args = parser.parse_args()
    print(args)
    path = args.Path
    bucket = args.Bucket
    key = args.Key

    # Check that path drives to a file.
    if not os.path.isfile(path):
        print('The path specified is not valid. Please, specify the path for the file to upload.')
        sys.exit()

    print(f'Path: {path} \nBucket: {bucket} \nKey: {key}.')

    # Returns parsed args.
    return path, bucket, key


def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.

    'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.

    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n:
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y:
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True

    Function from: https://code.activestate.com/recipes/541096-prompt-the-user-for-confirmation/
    """

    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = input(prompt)
        print(ans)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print('please enter y or n.')
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False


def upload_file_s3(file_source, bucket, key):
    """
    Sube un fichero al bucket "input" del data lake. 
    La key debe tener estuctura: <device>/<traffic_type>/<attack>/<filename>.
    En caso de tráfico benigno, <traffic_type> y <attack> tendrán la etiqueta "benign_traffic": 
    """
    s3 = boto3.client('s3')
    try:

        # Check key structure.
        device, traffic_type, attack, filename = key.split("/")

        # Post file
        s3.upload_file(file_source, bucket, key)
        print(f'{key} uploaded to bucket.')
        print(f"""
        Device: {device}
        Traffic type: {traffic_type}
        Attack: {attack}
        File Name: {filename}
        """)

    except ClientError as e:
        print(e)


def main():
    # Get parsed args.
    path, bucket, key = parseArgs()
    object_key = "/".join(key)
    # Ask the user for confirmation of the input.
    confirmation = confirm(prompt='Please confirm the input is correct:')
    if confirmation:
        print('Uploading file to S3')
        upload_file_s3(path, bucket, object_key)

    else:
        print('Operation cancelled by the user')


if __name__ == "__main__":
    main()
