#!/usr/bin/env python3
try:
    import sys
    import traceback
    from time import time
    import boto3
    import os
    import argparse
    from concurrent_plugin import concurrent_core
    import boto3.session
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        from mypy_boto3_cloudwatch.client import CloudWatchClient
    else:
        CloudWatchClient = object

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

    parser = argparse.ArgumentParser(description="Reads cloudwatch logs and copies them for further processing downstream.")
    parser.add_argument('--aws_profile_iam_roles_anywhere', required=False, default='subscriber_cloudwatch_iam_roles_anywhere', 
                        help='Relies on aws profile named "subscriber_cloudwatch_iam_roles_anywhere" which uses IAM roles anywhere to access the cloudwatch logs in the subscriber account.  This profile needs to be setup before running this script')
    parser.add_argument('--groupname_startswith', help='only choose log group names that start with this', required=False)

    args = parser.parse_args()
    print(f"Using aws IAM roles anywhere profile={args.aws_profile_iam_roles_anywhere}")
    
    session:boto3.session.Session = boto3.session.Session(profile_name=args.aws_profile_iam_roles_anywhere)
    client:CloudWatchClient = session.client('logs', region_name='us-east-1')
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
                    print(f"Group name {group['logGroupName']} does not start with {args.groupname_startswith}. Skipping group..", flush=True)
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
    traceback.print_exception(*sys.exc_info())
os._exit(os.EX_OK)
