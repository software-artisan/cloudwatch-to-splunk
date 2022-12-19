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
  from concurrent_plugin import concurrent_core

  print('summation: Entered', flush=True)
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
  concurrent_core.concurrent_log_artifact("/tmp/summation.json", "")
  os._exit(os.EX_OK)
except Exception as e1:
  print("Caught " + str(e1), flush=True)
  os._exit(os.EX_OK)

