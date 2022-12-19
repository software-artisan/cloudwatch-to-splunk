#!/usr/bin/env python3

def process_one_log_group(client, log_group_name, region):
  ind = 0
  nextToken = None
  while True:
    if nextToken:
      rv = client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', descending=True, limit=50, nextToken=nextToken)
    else:
      rv = client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', descending=True, limit=50)
    log_streams = rv['logStreams']
    for one_stream in log_streams:
      fn = '/tmp/emptyfile-' + str(ind)
      open(fn, 'a').close()
      ind = ind + 1
      concurrent_core.concurrent_log_artifact(fn, "dummy", LogGroupName=log_group_name, LogStreamName=one_stream['logStreamName'], region=region)
      break
    break
    if ('nextToken' in rv):
      nextToken = rv['nextToken']
    else:
      break

try:
  from time import time
  import boto3
  import os
  import sys
  import datetime
  import tzlocal
  import argparse
  from concurrent_plugin import concurrent_core
  from transformers import pipeline
  from transformers import AutoTokenizer, AutoModelForTokenClassification
  import json
  import urllib
  import re
  from urllib.parse import quote

  parser = argparse.ArgumentParser()
  parser.add_argument('--access_key_id', help='aws access key id', required=True)
  parser.add_argument('--secret_access_key', help='aws secret access key', required=True)

  args = parser.parse_args()

  region = 'us-east-1'
  client = boto3.client('logs', region_name=region, aws_access_key_id=args.access_key_id, aws_secret_access_key=args.secret_access_key)

  df = concurrent_core.list(None)
  print('Column Names:', flush=True)
  cn = df.columns.values.tolist()
  print(str(cn))
  print('------------------------------ Start Input ----------------', flush=True)
  df.reset_index()
  for ind, row in df.iterrows():
    print("Input row=" + str(row), flush=True)
    process_one_log_group(client, row['LogGroupName'], region)
  print('------------------------------ Finished Input ----------------', flush=True)
  os._exit(os.EX_OK)
except Exception as e1:
  print("Caught " + str(e1), flush=True)
  os._exit(os.EX_OK)

