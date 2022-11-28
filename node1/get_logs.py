#!/usr/bin/env python3

try:
    from time import time
    import boto3
    import os
    import sys
    import datetime
    import tzlocal
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--access_key_id', help='aws access key id', required=True)
    parser.add_argument('--secret_access_key', help='aws secret access key', required=True)

    args = parser.parse_args()

    client = boto3.client('logs', region_name='us-east-1', aws_access_key_id=args.access_key_id, aws_secret_access_key=args.secret_access_key)
    paginator = client.get_paginator('describe_log_groups')
    for pg in paginator.paginate():
        print('pg=' + str(pg))
except Exception as e1:
    print('Caught ' + str(e1), flush=True)
os._exit(os.EX_OK)
