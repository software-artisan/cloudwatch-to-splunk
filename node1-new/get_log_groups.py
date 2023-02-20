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
    from infinstor import infin_boto3

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
    parser.add_argument('--bucket', help='output bucket name', required=True)
    parser.add_argument('--prefix', help='output prefix', required=True)

    args = parser.parse_args()

    if 'PERIODIC_RUN_FREQUENCY' in os.environ:
        print(f"PERIDOIC_RUN_FREQUENCY is {os.environ['PERIODIC_RUN_FREQUENCY']}", flush=True)
    else:
        print('PERIDOIC_RUN_FREQUENCY is not set')
    if 'PERIODIC_RUN_START_TIME' in os.environ:
        print(f"PERIDOIC_RUN_START_TIME is {os.environ['PERIODIC_RUN_START_TIME']}", flush=True)
    else:
        print('PERIDOIC_RUN_START_TIME is not set')
    if 'PERIODIC_RUN_END_TIME' in os.environ:
        print(f"PERIODIC_RUN_END_TIME is {os.environ['PERIODIC_RUN_END_TIME']}", flush=True)
    else:
        print('PERIODIC_RUN_END_TIME is not set')
    start_time = None
    end_time = None
    periodic_run_frequency = os.getenv('PERIODIC_RUN_FREQUENCY')
    periodic_run_start_time = os.getenv('PERIODIC_RUN_START_TIME')
    periodic_run_end_time = os.getenv('PERIODIC_RUN_END_TIME')
    if periodic_run_frequency and periodic_run_start_time and periodic_run_end_time:
        start_time = datetime.fromtimestamp(int(periodic_run_start_time), tz=timezone.utc)
        end_time = datetime.fromtimestamp(int(periodic_run_end_time), tz=timezone.utc)
        print(f'Periodic Run with frequency {periodic_run_frequency}. start_time={start_time} --> end_time={end_time}')

    groups_file = open('/tmp/log_groups.txt', 'w')
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
                    groups_file.write(group['logGroupName'] + '\n')
        if 'NextToken' in page_iterator:
            nextToken = page_iterator['NextToken']
        else:
            break
    start_time_epochms = int(start_time.timestamp() * 1000)
    end_time_epochms = int(end_time.timestamp() * 1000)
    groups_file.close()
    s3client = boto3.client('s3')
    obj_name = args.prefix.lstrip('/').rstrip('/') + '/index/log_groups.txt'
    print(f"Object Name = {obj_name}")
    response = s3client.upload_file('/tmp/log_groups.txt', args.bucket, obj_name,
            ExtraArgs={"Metadata": {"infinsnap_start": str(start_time_epochms), "infinsnap_end": str(end_time_epochms)}})
except Exception as e1:
    print('Caught ' + str(e1), flush=True)
os._exit(os.EX_OK)
