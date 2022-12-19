#!/usr/bin/env python3

def quote_once(s):
    return quote(s, safe="").replace("%", "$")

def nested_quote(s):
    return quote(quote(s, safe="")).replace("%", "$")

def aws_cloudwatch_url(region, log_group, log_stream, dt):
    return "/".join([
        f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:log-groups",
        "log-group",
        nested_quote(log_group),
        "log-events",
        nested_quote(log_stream) + quote_once('?start=') + nested_quote(str(dt)),
    ])

def add_log_line(dt, person, log_line, all_messages, log_group, log_stream, region):
  cw_url = aws_cloudwatch_url(region, log_group, log_stream, dt)
  print('CloudWatch URL is: ' + cw_url)
  if person in all_messages:
    all_messages[person].append(cw_url)
  else:
    all_messages[person] = [log_line]

def process_one_log_stream_inner(client, ner, fp, group_name, stream_name, region, all_messages):
    print(f'Dumping log stream inner: {fp.name}')
    nt = None
    while (True):
        print('.... nt=' + str(nt))
        if nt:
            resp = client.get_log_events(logGroupName=group_name, logStreamName=stream_name, startFromHead=False, nextToken=nt)
        else:
            resp = client.get_log_events(logGroupName=group_name, logStreamName=stream_name, startFromHead=True)

        events = resp['events']
        for event in events:
            dt = datetime.datetime.fromtimestamp(event['timestamp']/1000, datetime.timezone.utc)
            msg = event['message']
            # fp.write(str(dt) + ' : ' + msg)
            s = ner(msg)
            orgs = []
            persons = []
            misc = []
            for entry in s:
              print("ner ret: Entry=" + str(entry))
              if entry['entity_group'] == 'ORG':
                orgs.append(entry['word'])
              elif entry['entity_group'] == 'PER':
                persons.append(entry['word'])
                add_log_line(dt, entry['word'], msg, all_messages, group_name, stream_name, region)
              elif entry['entity_group'] == 'MISC':
                misc.append(entry['word'])
            print(str(dt) + ": orgs=" + str(orgs) + ", persons=" + str(persons) + ", misc=" + str(misc) + " : " + msg)
        if ('nextForwardToken' in resp):
          if nt == resp['nextForwardToken']:
            break
          else:
            nt = resp['nextForwardToken']
        else:
          break

def process_one_log_stream(client, ner, stream_name, log_group_name, region, creation_time, all_messages):
    datetime_local:datetime.datetime = datetime.datetime.fromtimestamp(creation_time/1000, tz=tzlocal.get_localzone())  #tz=datetime.timezone.utc
    fname = (str(datetime_local) + '_' + stream_name ).replace("/","_").replace(" ","_")
    print(f"Dumping log stream {stream_name} to file /tmp/{fname}", flush=True)
    with open('/tmp/' + fname, 'w') as fp:
        process_one_log_stream_inner(client, ner, fp, log_group_name, stream_name, region, all_messages)

def process_one_log_group(client, ner, log_group_name, region, all_messages):
  nextToken = None
  while True:
    if nextToken:
      rv = client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', descending=True, limit=50, nextToken=nextToken)
    else:
      rv = client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', descending=True, limit=50)
    log_streams = rv['logStreams']
    for one_stream in log_streams:
      process_one_log_stream(client, ner, one_stream['logStreamName'], log_group_name, region, one_stream['creationTime'], all_messages)
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

  print('------------------------------ Begin Loading Huggingface ner model ------------------', flush=True)
  try:
    tokenizer = AutoTokenizer.from_pretrained("Jean-Baptiste/roberta-large-ner-english")
    model = AutoModelForTokenClassification.from_pretrained("Jean-Baptiste/roberta-large-ner-english")
  except Exception as err:
    print('Caught ' + str(err) + ' while loading ner model')
  print('------------------------------ After Loading Huggingface ner model ------------------', flush=True)

  print('------------------------------ Begin Creating Huggingface ner pipeline ------------------', flush=True)
  ner = pipeline('ner', model=model, tokenizer=tokenizer, aggregation_strategy="simple")
  print('------------------------------ After Creating Huggingface ner pipeline ------------------', flush=True)

  region = 'us-east-1'
  client = boto3.client('logs', region_name=region, aws_access_key_id=args.access_key_id, aws_secret_access_key=args.secret_access_key)

  df = concurrent_core.list(None)
  print('Column Names:', flush=True)
  cn = df.columns.values.tolist()
  print(str(cn))
  print('------------------------------ Start Input ----------------', flush=True)
  df.reset_index()

  all_messages = {}
  for ind, row in df.iterrows():
    print("Input row=" + str(row), flush=True)
  print('------------------------------ Finished Input ----------------', flush=True)
  os._exit(os.EX_OK)
except Exception as e1:
  print("Caught " + str(e1), flush=True)
  os._exit(os.EX_OK)

