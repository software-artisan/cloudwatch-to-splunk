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

def process_one_log_stream(client, ner, qa_pipeline, group_name, stream_name, first_event_time, last_event_time,
                            region, s3client, bucket, prefix, start_time_epochms, end_time_epochms):
    print(f"process_one_log_stream: Entered. grp={group_name}, strm={stream_name}", flush=True)
    print(f"  first_event_time={first_event_time}, last_event_time={last_event_time} output=s3://{bucket}/{prefix}", flush=True)
    print(f"  start_time_epochs={start_time_epochms}, end_time_epochs={end_time_epochms}", flush=True)
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
        for event in events:
            inp_txt = event['message']
            print(f"message={event['message']}")
            if inp_txt:
              try:
                result = qa_pipeline(question="is this an error?", context=inp_txt)
                ans = result['answer']
                print(f"QA.is this an error?={ans} with certainity {result['score']")
                msg_list.append(inp_txt)
                timestamp_list.append(datetime.fromtimestamp(event['timestamp']/1000, timezone.utc))
              except Exception as ex:
                print(f"Caught {ex} while qa. Ignoring log event and continuing..")
        if not msg_list:
          print("No more messages to apply ner model to")
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
            print(str(timestamp_list[idx]) + ": orgs=" + str(orgs) + ", persons=" + str(persons) + ", misc=" + str(misc) + " : " + msg_list[idx])
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

def print_gpu_utilization():
    nvmlInit()
    handle = nvmlDeviceGetHandleByIndex(0)
    info = nvmlDeviceGetMemoryInfo(handle)
    print(f"GPU memory occupied: {info.used//1024**2} MB.")


def print_summary(result):
    print(f"Time: {result.metrics['train_runtime']:.2f}")
    print(f"Samples/second: {result.metrics['train_samples_per_second']:.2f}")
    print_gpu_utilization()

import re
scanner=re.Scanner([
  (r"\[[^\]]*\]",       lambda scanner,token: token),
  (r"\+",      lambda scanner,token:"R_PLUS"),
  (r"\*",        lambda scanner,token:"R_KLEENE"),
  (r"%",        lambda scanner,token:"R_WILD"),
  (r"\^",        lambda scanner,token:"R_START"),
  (r"\$",        lambda scanner,token:"R_END"),
  (r"\?",        lambda scanner,token:"R_QUESTION"),
  (r"[\.~``;_a-zA-Z0-9\s=:\{\}\-\\]+", lambda scanner,token:"R_FREE"),
  (r'.', lambda scanner, token: None),
])

def tokenizeRegex(s):
  results, remainder=scanner.scan(s)
  return results

def my_traverse(token_list, statement_list, result_list):
  for t in token_list:
    if t.ttype == None:
      my_traverse(t, statement_list, result_list)
    elif t.ttype != sqlparse.tokens.Whitespace:
      statement_list.append(t.ttype)
      result_list.append(str(t))
  return statement_list, result_list

def sanitizeSql(sql):
  s = sql.strip().lower()
  if not s[-1] == ";":
    s += ';'
  s = re.sub(r'\(', r' ( ', s)
  s = re.sub(r'\)', r' ) ', s)
  s = s.replace('#', '')
  return s

def tokenizeSql(code):
    statement_list = []
    result_list = []
    code = sanitizeSql(code)
    print(f"Sanitized SQL={code}")
    tokens = sqlparse.parse(code)
    print(f"Tokens={tokens}")
    statements, result = my_traverse(tokens, statement_list, result_list)
    print(f"Statements={statements}, result={result}")

    table_map = {}
    column_map = {}
    for i in range(len(statements)):
      print(f"i={i}, stmt={statements[i]} result={result[i]}")
      if statements[i] in [sqlparse.tokens.Number.Integer, sqlparse.tokens.Literal.Number.Integer]:
        print(f"CODE_INTEGER")
        result[i] = "CODE_INTEGER"
      elif statements[i] in [sqlparse.tokens.Number.Float, sqlparse.tokens.Literal.Number.Float]:
        print(f"CODE_FLOAT")
        result[i] = "CODE_FLOAT"
      elif statements[i] in [sqlparse.tokens.Number.Hexadecimal, sqlparse.tokens.Literal.Number.Hexadecimal]:
        print(f"CODE_HEX")
        result[i] = "CODE_HEX"
      elif statements[i] in [sqlparse.tokens.String.Symbol, sqlparse.tokens.String.Single, sqlparse.tokens.Literal.String.Symbol]:
        result[i] = tokenizeRegex(result[i])
        print(f"TokenizedRegex={result[i]}")
      elif statements[i] in [sqlparse.tokens.Literal.String.Single]:
        result[i] = tokenizeRegex(result[i])[0]
        print(f"TokenizedRegex single={result[i]}")
      elif statements[i] in[sqlparse.tokens.Name, sqlparse.tokens.Name.Placeholder, sqlparse.sql.Identifier]:
        print("AAAAA")
        old_value = result[i]
        if old_value in column_map:
          result[i] = column_map[old_value]
        else:
          result[i] = 'col'+ str(len(column_map))
          column_map[old_value] = result[i]
      elif (result[i] == "." and statements[i] == sqlparse.tokens.Punctuation and i > 0 and result[i-1].startswith('col')):
        print("BBBBB")
        old_value = result[i-1]
        if old_value in table_map:
          result[i-1] = table_map[old_value]
        else:
          result[i-1] = 'tab'+ str(len(table_map))
          table_map[old_value] = result[i-1]
      if (result[i].startswith('col') and i > 0 and (result[i-1] in ["from"])):
        print("CCCCC")
        old_value = result[i]
        if old_value in table_map:
          result[i] = table_map[old_value]
        else:
          result[i] = 'tab'+ str(len(table_map))
          table_map[old_value] = result[i]

    tokenized_code = ' '.join(result)
    print("SQL after tokenized: " + tokenized_code)
    return tokenized_code

try:
  from time import time
  import boto3
  import os
  import sys
  from datetime import datetime, timezone, timedelta
  import tzlocal
  import argparse
  from concurrent_plugin import concurrent_core
  from transformers import pipeline
  from transformers import AutoTokenizer, AutoModelForTokenClassification
  import json
  import urllib
  from urllib.parse import quote
  from infinstor import infin_boto3
  import torch
  from pynvml import *
  import sqlparse


  parser = argparse.ArgumentParser()
  parser.add_argument('--access_key_id', help='aws access key id', required=True)
  parser.add_argument('--secret_access_key', help='aws secret access key', required=True)
  parser.add_argument('--bucket', help='output bucket name', required=True)
  parser.add_argument('--prefix', help='output prefix', required=True)

  args = parser.parse_args()

  print('------------------------------ Begin Loading Huggingface QA pipeline ------------------', flush=True)
  try:
    from transformers import pipeline
    qa_pipeline = pipeline("question-answering", model="distilbert-base-cased-distilled-squad", revision="626af31")
  except Exception as err:
    print('Caught ' + str(err) + ' while loading QA pipeline', flush=True)
    os._exit(os.EX_OK)
  print('------------------------------ Finished Loading Huggingface QA Pipelein ------------------', flush=True)

  print_gpu_utilization()

  print('------------------------------ Begin Loading Huggingface ner model ------------------', flush=True)
  try:
    tokenizer = AutoTokenizer.from_pretrained("Jean-Baptiste/roberta-large-ner-english")
    model = AutoModelForTokenClassification.from_pretrained("Jean-Baptiste/roberta-large-ner-english").to('cuda')
  except Exception as err:
    print('Caught ' + str(err) + ' while loading ner model', flush=True)
    os._exit(os.EX_OK)
  print('------------------------------ After Loading Huggingface ner model ------------------', flush=True)

  print_gpu_utilization()

  print('------------------------------ Begin Creating Huggingface ner pipeline ------------------', flush=True)
  ner = pipeline('ner', model=model, tokenizer=tokenizer, aggregation_strategy="simple", device="cuda:0")
  print('------------------------------ After Creating Huggingface ner pipeline ------------------', flush=True)

  print_gpu_utilization()

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
        process_one_log_stream(client, ner, qa_pipeline, row['LogGroupName'], row['LogStreamName'],
                        row['LogStreamFirstEventTime'], row['LogStreamLastEventTime'], row['region'],
                        s3client, args.bucket, args.prefix, int(start_time.timestamp() * 1000), int(periodic_run_start_time))
    except Exception as e2:
      print(f"Caught {e2} processing log stream. Ignoring and continuing to next log stream.." , flush=True)
  print('------------------------------ Finished Input ----------------', flush=True)

  os._exit(os.EX_OK)
except Exception as e1:
  print("Caught " + str(e1), flush=True)
  os._exit(os.EX_OK)

