#!/usr/bin/env python3

def union(summation, dct):
  for ky in dct:
    if ky in summation:
      summation[ky] = summation[ky] + dct[ky]
    else:
      summation[ky] = dct[ky]

try:
  import mlflow
  import os
  import infinstor_mlflow_plugin
  import boto3
  import tempfile
  import pandas as pd
  import json
  import sys
  import time
  import argparse
  from concurrent_plugin import concurrent_core
  from infinstor import infin_boto3

  print('summation: Entered', flush=True)
  if 'PERIODIC_RUN_START_TIME' in os.environ:
    print(f"PERIDOIC_RUN_START_TIME is {os.environ['PERIODIC_RUN_START_TIME']}", flush=True)
    periodic_run_start_time = os.getenv('PERIODIC_RUN_START_TIME')
  else:
    print('PERIDOIC_RUN_START_TIME is not set. Using current time')
    periodic_run_start_time = time.time_ns()//1_000_000

  parser = argparse.ArgumentParser()
  parser.add_argument('--bucket', help='output bucket name', required=True)
  parser.add_argument('--prefix', help='output prefix', required=True)

  args = parser.parse_args()

  df = concurrent_core.list(None, input_name='input1')

  print('Concurrent Core DataFrame Columns:', flush=True)
  cn = df.columns.values.tolist()
  print(str(cn))

  lp = concurrent_core.get_local_paths(df)
  print('Concurrent Core Local paths: num files=' + str(len(lp)))
  summation = None
  for one_file in lp:
      print(str(one_file), flush=True)
      with open(one_file, 'r') as fp:
        dct = json.load(fp)
        if summation == None:
          summation = dct
        else:
          union(summation, dct)
  with open("/tmp/summation.json", 'w') as fp:
    json.dump(summation, fp)

  client = boto3.client('s3')
  obj_name = args.prefix.lstrip('/').rstrip('/') + '/' + str(periodic_run_start_time) + '/summation.json'
  response = client.upload_file('/tmp/summation.json', args.bucket, obj_name)

  concurrent_core.concurrent_log_artifact("/tmp/summation.json", "")
  os._exit(os.EX_OK)
except Exception as e1:
  print("Caught " + str(e1), flush=True)
  os._exit(os.EX_SOFTWARE)
