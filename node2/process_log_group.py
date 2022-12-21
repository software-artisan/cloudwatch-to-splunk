#!/usr/bin/env python3

def overlap(start_time, end_time, stream_first_time, stream_last_time):
  sa = []
  if start_time < stream_first_time:
    sa.append([start_time, end_time])
    sa.append([stream_first_time, stream_last_time])
  else:
    sa.append([stream_first_time, stream_last_time])
    sa.append([start_time, end_time])
  if sa[1][0] < sa[0][1]:
    return True
  return False

def process_one_log_group(client, log_group_name, region, start_time, end_time):
  ind = 0
  nextToken = None
  while True:
    if nextToken:
      rv = client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', descending=True, limit=50, nextToken=nextToken)
    else:
      rv = client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', descending=True, limit=50)
    log_streams = rv['logStreams']
    for one_stream in log_streams:
      stream_first_time = datetime.fromtimestamp(one_stream['firstEventTimestamp'], tz=timezone.utc)
      stream_last_time = datetime.fromtimestamp(one_stream['lastEventTimestamp'], tz=timezone.utc)
      print(f'log_stream [{stream_first_time} -> {stream_last_time}]', flush=True)
      if start_time and end_time:
        print(f'start_time and end_time are defined: [{start_time} -> {end_time}]', flush=True)
        if not overlap(start_time, end_time, stream_first_time, stream_last_time):
          print(f'No overlap between periodic_run [{start_time} -> {end_time}] and stream [{stream_first_time} -> {stream_last_time}]. Done with streams', flush=True)
          return
      fn = '/tmp/' + log_group_name.replace('/', '-') + '-' + str(ind)
      open(fn, 'a').close()
      ind = ind + 1
      #if ind > 1: # For testing. restrict to two log streams per log group
        #return
      concurrent_core.concurrent_log_artifact(fn, "marker", LogGroupName=log_group_name, LogStreamName=one_stream['logStreamName'], region=region)
    if ('nextToken' in rv):
      nextToken = rv['nextToken']
    else:
      break

import traceback
try:
  from time import time
  import boto3
  import os
  import sys
  from datetime import date, datetime, timezone, timedelta
  import tzlocal
  import argparse
  from concurrent_plugin import concurrent_core
  from transformers import pipeline
  from transformers import AutoTokenizer, AutoModelForTokenClassification
  import json
  import urllib
  import re
  from urllib.parse import quote

  if 'PERIODIC_RUN_FREQUENCY' in os.environ:
    print(f"PERIDOIC_RUN_FREQUENCY is {os.environ['PERIODIC_RUN_FREQUENCY']}", flush=True)
  else:
    print('PERIDOIC_RUN_FREQUENCY is not set')
  if 'PERIODIC_RUN_START_TIME' in os.environ:
    print(f"PERIDOIC_RUN_START_TIME is {os.environ['PERIODIC_RUN_START_TIME']}", flush=True)
  else:
    print('PERIDOIC_RUN_START_TIME is not set')
  start_time = None
  end_time = None
  periodic_run_frequency = os.getenv('PERIODIC_RUN_FREQUENCY')
  periodic_run_start_time = os.getenv('PERIODIC_RUN_START_TIME')
  if periodic_run_frequency and periodic_run_start_time:
    end_time = datetime.fromtimestamp(int(periodic_run_start_time)/1000, tz=timezone.utc)
    if periodic_run_frequency == 'hourly':
        start_time = end_time - timedelta(hours=1)
    elif periodic_run_frequency == 'daily':
        start_time = end_time - timedelta(days=1)
    elif periodic_run_frequency == 'weekly':
        start_time = end_time - timedelta(days=7)
    elif periodic_run_frequency == 'monthly':
        start_time = end_time - timedelta(months=1)
    elif periodic_run_frequency == 'yearly':
        start_time = end_time - timedelta(years=1)
    else:
      print('Error. Unknown periodic_run_frequency ' + str(periodic_run_frequency), flush=True)
      start_time = None
      end_time = None
    print(f'Periodic Run with frequency {periodic_run_frequency}. start_time={start_time} --> end_time={end_time}')

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
    process_one_log_group(client, row['LogGroupName'], region, start_time, end_time)
  print('------------------------------ Finished Input ----------------', flush=True)
  os._exit(os.EX_OK)
except Exception as e1:
  print("Caught " + str(e1), flush=True)
  traceback.print_tb(e1)
  os._exit(os.EX_OK)

