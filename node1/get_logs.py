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

  parser = argparse.ArgumentParser()
  parser.add_argument('--access_key_id', help='aws access key id', required=True)
  parser.add_argument('--secret_access_key', help='aws secret access key', required=True)

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
        print(group['logGroupName'], flush=True)
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
