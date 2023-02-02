#!/usr/bin/env python3

articles = ['a', 'an', 'the']
demonstrative_pronouns = ['this', 'that', 'these', 'those']
misc = ['there']

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

def add_log_line(dt, msg, person, all_messages, log_group, log_stream, region):
    cw_url = aws_cloudwatch_url(region, log_group, log_stream, dt)
    if person in all_messages:
        all_messages[person].append((dt.timestamp(), cw_url, msg))
    else:
        all_messages[person] = [(dt.timestamp(), cw_url, msg)]
    elen = len(person) + 16 + len(cw_url) + len(msg)
    elen = elen + (elen * 0.2)
    return elen

def do_flair(tagger, tm, msg, all_messages, group_name, stream_name, region):
    slen = 0
    sentence = Sentence(msg.strip())
    before = datetime.utcnow()
    tagger.predict(sentence)
    delta = datetime.utcnow() - before
    print(f"Time to run flair on msg: {delta}")
    for entity in sentence.get_spans('np'):
        if entity.tag != 'NP':
            continue
        ent = entity.text.strip().lower()
        if ent not in articles and ent not in demonstrative_pronouns and ent not in misc:
            llen = add_log_line(tm, msg, ent, all_messages, group_name, stream_name, region)
            slen = slen + llen
    return slen

def extract_path(msg):
    # e.g. blah.blah.blah, 'path': '/2.0/mlflow/parallels/list-periodicruns',
    # e.g. blah.blah.blah, "path":"/customerinfo",
    print(f"extract_path: Entered. msg={msg}", flush=True)
    match = re.search("path' *: *'([^']*)", msg)
    if match:
        print("found", match.group(1))
        return match.group(1)
    else:
        print("path with single quotes not found")
    match = re.search('path" *: *"([^"]*)', msg)
    if match:
        print("found", match.group(1))
        return match.group(1)
    else:
        print("path with double quotes not found")
    return None

def process_one_log_stream(client, tagger, ner, group_name, stream_name, first_event_time, last_event_time,
                            region, s3client, bucket, prefix, start_time_epochms, end_time_epochms):
    print(f"process_one_log_stream: Entered. grp={group_name}, strm={stream_name}", flush=True)
    print(f"  first_event_time={first_event_time}, last_event_time={last_event_time} output=s3://{bucket}/{prefix}", flush=True)
    print(f"  start_time_epochs={start_time_epochms}, end_time_epochs={end_time_epochms}", flush=True)
    total_len = 0
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
            msg = event['message']
            tm = datetime.fromtimestamp(event['timestamp']/1000, timezone.utc)
            print(f"Processing msg={msg}")
            tlen = do_flair(tagger, tm, msg, all_messages, group_name, stream_name, region)
            total_len = total_len + tlen
            # run NER through every line
            msg_list.append(msg)
            timestamp_list.append(tm)
        print(f"Finished flair model. total_len={total_len}, all_messages len={len(all_messages)}")

        if not msg_list:
            print("No more messages to apply ner model to")
            break
        else:
            print(f"Applying NER model to {len(all_messages)} messages")
        before = datetime.utcnow()
        output_list = ner(msg_list)
        delta = datetime.utcnow() - before
        print(f"Time to run ner on {len(msg_list)} msgs: {delta}")
        for idx, one_output in enumerate(output_list):
            misc = []
            for entry in one_output:
                print("ner ret: Entry=" + str(entry))
                s_entry_word = entry['word'].strip()
                if entry['entity_group'] == 'ORG':
                    alen = add_log_line(timestamp_list[idx], msg_list[idx], s_entry_word, all_messages, group_name, stream_name, region)
                    total_len = total_len + alen
                elif entry['entity_group'] == 'PER':
                    alen = add_log_line(timestamp_list[idx], msg_list[idx], s_entry_word, all_messages, group_name, stream_name, region)
                    total_len = total_len + alen
                elif entry['entity_group'] == 'MISC':
                    misc.append(s_entry_word)
        print(f"Finished NER model. total_len={total_len}, all_messages len={len(all_messages)}")

        if ('nextForwardToken' in resp):
            if nt == resp['nextForwardToken']:
                break
            else:
                nt = resp['nextForwardToken']
        else:
            break
    if all_messages:
        fn = group_name.replace('/', '_') + '-' + stream_name.replace('/', '_') + '-' + str(first_event_time) + '.json'
        with open(fn, 'w') as fp:
            json.dump(all_messages, fp, ensure_ascii=True, indent=4, sort_keys=True)
        print(f"File Name = {fn}")
        obj_name = prefix.lstrip('/').rstrip('/') + '/' + fn.lstrip('/')
        print(f"Object Name = {obj_name}")
        response = s3client.upload_file(fn, bucket, obj_name, ExtraArgs={"Metadata": {"infinsnap": str(first_event_time)}})

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
    import re
    from urllib.parse import quote
    from infinstor import infin_boto3
    from flair.data import Sentence
    from flair.models import SequenceTagger

    parser = argparse.ArgumentParser()
    parser.add_argument('--access_key_id', help='aws access key id', required=True)
    parser.add_argument('--secret_access_key', help='aws secret access key', required=True)
    parser.add_argument('--bucket', help='output bucket name', required=True)
    parser.add_argument('--prefix', help='output prefix', required=True)
    parser.add_argument('--override_start_time', help='set to override periodic run start time', required=True)

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
    if args.override_start_time != "ignore":
        start_time = datetime.fromtimestamp(int(args.override_start_time), tz=timezone.utc)
        print(f'Overriding Periodic Run Start Time using parameter. New Start Time={start_time}')

    print('------------------------------ Begin Loading Huggingface ner model ------------------', flush=True)
    try:
        tokenizer = AutoTokenizer.from_pretrained("Jean-Baptiste/roberta-large-ner-english")
        model = AutoModelForTokenClassification.from_pretrained("Jean-Baptiste/roberta-large-ner-english")
    except Exception as err:
        print('Caught ' + str(err) + ' while loading ner model')
    print('------------------------------ After Loading Huggingface ner model ------------------', flush=True)

    print('------------------------------ Begin Creating Huggingface ner pipeline ------------------', flush=True)
    ner = pipeline('ner', model=model, tokenizer=tokenizer, aggregation_strategy="max")
    print('------------------------------ After Creating Huggingface ner pipeline ------------------', flush=True)

    print('------------------------------ Begin Creating Huggingface SequenceTagger ------------------', flush=True)
    tagger = SequenceTagger.load("flair/chunk-english")
    print('------------------------------ After Creating Huggingface SequenceTagger ------------------', flush=True)

    region = 'us-east-1'
    client = boto3.client('logs', region_name=region, aws_access_key_id=args.access_key_id, aws_secret_access_key=args.secret_access_key)

    print('------------------------------ Before concurrent_core.list ------------------', flush=True)
    df = concurrent_core.list(None)
    print('------------------------------ After concurrent_core.list ------------------', flush=True)

    print('------------------------------ Start Processing LogStreams ----------------', flush=True)
    df.reset_index()
    s3client = boto3.client('s3')

    for ind, row in df.iterrows():
        try:
            process_one_log_stream(client, tagger, ner, row['LogGroupName'], row['LogStreamName'],
                            row['LogStreamFirstEventTime'], row['LogStreamLastEventTime'], row['region'],
                            s3client, args.bucket, args.prefix, int(start_time.timestamp() * 1000), int(end_time.timestamp() * 1000))
        except Exception as e2:
            print(f"Caught {e2} processing log stream. Ignoring and continuing to next log stream.." , flush=True)
    print('------------------------------ Finished Processing LogStreams. Program finished ----------------', flush=True)
    os._exit(os.EX_OK)
except Exception as e1:
    print("Caught " + str(e1), flush=True)
    os._exit(os.EX_OK)
