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

def add_log_line(dt, person, all_messages, log_group, log_stream, region):
  cw_url = aws_cloudwatch_url(region, log_group, log_stream, dt)
  print('CloudWatch URL is: ' + cw_url)
  if person in all_messages:
    all_messages[person].append((dt.timestamp(), cw_url))
  else:
    all_messages[person] = [(dt.timestamp(), cw_url)]

def process_one_log_stream(client, ner, group_name, stream_name, first_event_time, last_event_time,
                            region, s3client, bucket, prefix, start_time_epochms, end_time_epochms):
    print(f"process_one_log_stream: Entered. grp={group_name}, strm={stream_name}", flush=True)
    print(f"  first_event_time={first_event_time}, last_event_time={last_event_time} output=s3://{bucket}/{prefix}", flush=True)
    print(f"  start_time_epochs={start_time_epochs}, end_time_epochs={end_time_epochs}", flush=True)
    all_messages = {}
    nt = None
    while (True):
        print(f".... nt={nt}, len_all_msgs={len(all_messages)}", flush=True)
        if nt:
            resp = client.get_log_events(logGroupName=group_name, logStreamName=stream_name,
                    startTime=start_time_epochms, endTime=end_time_epochms,
                    startFromHead=True, nextToken=nt, unmask=True)
        else:
            resp = client.get_log_events(logGroupName=group_name, logStreamName=stream_name,
                    startTime=start_time_epochms, endTime=end_time_epochms,
                    startFromHead=True, unmask=True)

        events = resp['events']
        msg_list = []
        timestamp_list = []
        for idx, event in enumerate(events):
            msg_list.append(event['message'])
            timestamp_list.append(datetime.datetime.fromtimestamp(event['timestamp']/1000, datetime.timezone.utc))
        if not msg_list:
          print("No more messages to apply model")
          break
        output_list = ner(msg_list)
        for idx, one_output in enumerate(output_list):
            orgs = []
            persons = []
            misc = []
            for entry in one_output:
              print("ner ret: Entry=" + str(entry))
              s_entry_word = entry['word'].strip()
              if entry['entity_group'] == 'ORG':
                orgs.append(s_entry_word)
              elif entry['entity_group'] == 'PER':
                persons.append(s_entry_word)
                add_log_line(timestamp_list[idx], s_entry_word, all_messages, group_name, stream_name, region)
              elif entry['entity_group'] == 'MISC':
                misc.append(s_entry_word)
            print(str(timestamp_list[idx]) + ": orgs=" + str(orgs) + ", persons=" + str(persons) + ", misc=" + str(misc) + " : " + msg)
        if ('nextForwardToken' in resp):
          if nt == resp['nextForwardToken']:
            break
          else:
            nt = resp['nextForwardToken']
        else:
          break
    if all_messages:
      fn = group_name.replace('/', '_') + '-' + stream_name.replace('/', '_') + '.json'
      with open(fn, 'w') as fp:
        json.dump(all_messages, fp)
      print(f"File Name = {fn}")
      obj_name = prefix.lstrip('/').rstrip('/') + '/' + fn.lstrip('/')
      print(f"Object Name = {obj_name}")
      response = s3client.upload_file(fn, bucket, obj_name, ExtraArgs={"Metadata": {"infinsnap": str(first_event_time)}})

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
  from infinstor import infin_boto3

  parser = argparse.ArgumentParser()
  parser.add_argument('--access_key_id', help='aws access key id', required=True)
  parser.add_argument('--secret_access_key', help='aws secret access key', required=True)
  parser.add_argument('--bucket', help='output bucket name', required=True)
  parser.add_argument('--prefix', help='output prefix', required=True)

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
        start_time = end_time - relativedelta(months=1)
    elif periodic_run_frequency == 'yearly':
        start_time = end_time - relativedelta(years=1)
    else:
      print('Error. Unknown periodic_run_frequency ' + str(periodic_run_frequency), flush=True)
      start_time = None
      end_time = None
    print(f'Periodic Run with frequency {periodic_run_frequency}. start_time={start_time} --> end_time={end_time}')

  s3client = boto3.client('s3')

  for ind, row in df.iterrows():
    print("Input row=" + str(row), flush=True)
    try:
        process_one_log_stream(client, ner, row['LogGroupName'], row['LogStreamName'],
                        row['LogStreamFirstEventTime'], row['LogStreamLastEventTime'], row['region'],
                        s3client, args.bucket, args.prefix, int(periodic_run_start_time), end_time.timestamp() * 1000)
    except Exception as e2:
      print(f"Caught {e2} processing log stream. Ignoring and continuing to next log stream.." , flush=True)
  print('------------------------------ Finished Input ----------------', flush=True)

  os._exit(os.EX_OK)
except Exception as e1:
  print("Caught " + str(e1), flush=True)
  os._exit(os.EX_OK)

