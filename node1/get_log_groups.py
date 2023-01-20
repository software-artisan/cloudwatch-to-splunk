#!/usr/bin/env python3

try:
    from time import time
    import boto3
    import os
    import sys
    import datetime
    import tzlocal
    import argparse
    from concurrent_plugin import concurrent_core

    if 'PERIODIC_RUN_FREQUENCY' in os.environ:
        print(f"PERIDOIC_RUN_FREQUENCY is {os.environ['PERIODIC_RUN_FREQUENCY']}", flush=True)
    else:
        print('PERIDOIC_RUN_FREQUENCY is not set')
    if 'PERIODIC_RUN_START_TIME' in os.environ:
        print(f"PERIDOIC_RUN_START_TIME is {os.environ['PERIODIC_RUN_START_TIME']}", flush=True)
    else:
        print('PERIDOIC_RUN_START_TIME is not set')
    periodic_run_frequency = os.getenv('PERIODIC_RUN_FREQUENCY')
    periodic_run_start_time = os.getenv('PERIODIC_RUN_START_TIME')

    parser = argparse.ArgumentParser()
    parser.add_argument('--access_key_id', help='aws access key id', required=True)
    parser.add_argument('--secret_access_key', help='aws secret access key', required=True)
    parser.add_argument('--groupname_startswith', help='only choose log group names that start with this', required=False)

    args = parser.parse_args()

    client = boto3.client('logs', region_name='us-east-1', aws_access_key_id=args.access_key_id, aws_secret_access_key=args.secret_access_key)
    paginator = client.get_paginator('describe_log_groups')
    nextToken = None
    ind = 0
    while True:
        if nextToken:
            page_iterator = paginator.paginate(limit=50, nextToken=nextToken)
        else:
            page_iterator = paginator.paginate(limit=50)
        for pg in page_iterator:
            print('pg=' + str(pg), flush=True)
            for group in pg['logGroups']:
                if args.groupname_startswith and not group['logGroupName'].startswith(args.groupname_startswith):
                    print(f"Group name {group['logGroupName']} does not start with {args.groupname_startswith}. Skipping group..")
                else:
                    print(f"Choosing group {group['logGroupName']}", flush=True)
                    fn = '/tmp/emptyfile-' + str(ind)
                    open(fn, 'a').close()
                    ind = ind + 1
                    concurrent_core.concurrent_log_artifact(fn, "dummy", LogGroupName=group['logGroupName'])
        if 'NextToken' in page_iterator:
            nextToken = page_iterator['NextToken']
        else:
            break
except Exception as e1:
    print('Caught ' + str(e1), flush=True)
os._exit(os.EX_OK)
