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
        ent = entity.text.strip("\"' \t-").lower()
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

skip_indices = set([0])

def extract_metrics(event, unique_keys):
    line = f"EVENTS {event['timestamp']} {event['message']}"
    #tokens =  nltk.word_tokenize("EVENTS  1675666424284   2023-02-06T06:53:43.250943Z   12 Query  INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1675666423240) ON DUPLICATE KEY UPDATE value = 1675666423240      1675666423250")
    tokens = nltk.word_tokenize(line)
    timestamps = []
    text_strings = []
    numbers = []
    for idx, t in enumerate(tokens):
        if idx in skip_indices:
            continue
        #Parse timestamp
        try:
            ts = parse_timestamp(t)
            timestamps.append(ts)
            continue
        except Exception as ex:
            pass

        parts = re.split(',| |_|-|!|:|\)|\(|=|"|\'', t)
        for part in parts:
            part = re.sub(r'[^\w\s]','',part)
            part = part.strip().lower()
            if not part:
                continue
            if part in stopwords.words('english'):
                continue
            try:
                ts = parse_timestamp(t)
                timestamps.append(ts)
            except:
                try:
                    num = float(part)
                    numbers.append(num)
                except Exception as ex:
                    text_strings.append(part)
    log_key = ' '.join(text_strings)
    print(log_key, numbers, timestamps)
    if log_key not in unique_keys:
        unique_keys[log_key] = [(event['timestamp'], event['message'], numbers, timestamps)]
    else:
        unique_keys[log_key].append((event['timestamp'], event['message'], numbers, timestamps))

def parse_timestamp(t):
    try:
        ts = pd.to_datetime(t)
    except Exception as ex:
        num = float(t)
        curr_time = time.time()
        if curr_time - 365*24*60*60 < num < curr_time + 365*24*60*60:
            ts = pd.to_datetime(num)
        elif curr_time - 365*24*60*60 < num / 1e3 < curr_time + 365*24*60*60:
            ts = pd.to_datetime(num, unit='ms')
        elif curr_time - 365*24*60*60 < num / 1e6 < curr_time + 365*24*60*60:
            ts = pd.to_datetime(num, unit='us')
        elif curr_time - 365 * 24 * 60 * 60 < num / 1e9 < curr_time + 365 * 24 * 60 * 60:
            ts = pd.to_datetime(num, unit='ns')
        else:
            raise Exception('Not a timestamp')
    return ts

def process_one_log_stream_sql(client, tagger, ner, group_name, stream_name, first_event_time, last_event_time,
                            region, s3client, bucket, prefix, start_time_epochms, end_time_epochms):
    print(f"process_one_log_stream_sql: Entered. grp={group_name}, strm={stream_name}", flush=True)
    total_len = 0
    all_messages = {}
    unique_keys = dict()
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
        for idx, event in enumerate(events):
            msg = event['message']
            tm = datetime.fromtimestamp(event['timestamp']/1000, timezone.utc)
            print(f"Processing msg={msg}")
            # extract metrics from message
            extract_metrics(event, unique_keys)
        print(f"Finished extracting metrics. Unique keys from metrics search={len(unique_keys)}")
        # unique keys is a dict that maps the extracted unique keys to an array of the following tuple:
        # (timestamp of line in epoch ms, message line, array of numbers extracted from line, array of timestamps extracted from line)

        for key, val in unique_keys.items():
            print(f"unique_key={key}: Number of lines with this unique key={len(val)}")
            for ov in val:
                print(f"  line={val[1]}, Numbers={val[2]}, Timestamps={val[3]}")

        print(f"Before applying NER to metrics search keys")
        before = datetime.utcnow()
        inp = list(unique_keys.keys())
        olist = ner(inp)
        for idx, one_output in enumerate(olist):
            misc = []
            for entry in one_output:
                s_entry_word = entry['word'].strip()
                if entry['entity_group'] == 'ORG':
                    print(f"Metrics ORG={s_entry_word} :: {inp[idx]}")
                elif entry['entity_group'] == 'PER':
                    print(f"Metrics PER={s_entry_word} :: {inp[idx]}")
                    for oi in unique_keys[inp[idx]]:
                        alen = add_log_line(oi[0], oi[1], s_entry_word, all_messages, group_name, stream_name, region)
                    total_len = total_len + alen
                elif entry['entity_group'] == 'MISC':
                    print(f"Metrics MISC={s_entry_word} :: {inp[idx]}")
        delta = datetime.utcnow() - before
        print(f"After applying NER to metrics search keys. Time={delta}")

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

def process_one_log_stream_general(client, tagger, ner, group_name, stream_name, first_event_time, last_event_time,
                            region, s3client, bucket, prefix, start_time_epochms, end_time_epochms):
    print(f"process_one_log_stream_general: Entered. grp={group_name}, strm={stream_name}", flush=True)
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
            print(f"TEMPORARILY DONE AFTER ONE MESSAGE")
            break
        print(f"Finished flair model. total_len={total_len}, all_messages len={len(all_messages)}")

        if not msg_list:
            print("No more messages to apply ner model to")
            break
        else:
            print(f"Applying NER model to {len(msg_list)} messages")
        before = datetime.utcnow()
        output_list = ner(msg_list)
        delta = datetime.utcnow() - before
        print(f"Time to run ner on {len(msg_list)} msgs: {delta}")
        for idx, one_output in enumerate(output_list):
            misc = []
            for entry in one_output:
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

def process_one_log_stream(client, tagger, ner, group_name, stream_name, first_event_time, last_event_time,
                            region, s3client, bucket, prefix, start_time_epochms, end_time_epochms):
    print(f"process_one_log_stream: Entered. grp={group_name}, strm={stream_name}", flush=True)
    print(f"  first_event_time={first_event_time}, last_event_time={last_event_time} output=s3://{bucket}/{prefix}", flush=True)
    print(f"  start_time_epochs={start_time_epochms}, end_time_epochs={end_time_epochms}", flush=True)
    if group_name.startswith("/aws/rds"):
        process_one_log_stream_sql(client, tagger, ner, group_name, stream_name, first_event_time, last_event_time,
                            region, s3client, bucket, prefix, start_time_epochms, end_time_epochms)
    else:
        process_one_log_stream_general(client, tagger, ner, group_name, stream_name, first_event_time, last_event_time,
                            region, s3client, bucket, prefix, start_time_epochms, end_time_epochms)
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
    import pandas as pd
    import nltk
    from nltk.corpus import stopwords
    import re
    import time

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
        model = AutoModelForTokenClassification.from_pretrained("Jean-Baptiste/roberta-large-ner-english").to('cuda')
    except Exception as err:
        print('Caught ' + str(err) + ' while loading ner model')
    print('------------------------------ After Loading Huggingface ner model ------------------', flush=True)

    print('------------------------------ Begin Creating Huggingface ner pipeline ------------------', flush=True)
    ner = pipeline('ner', model=model, tokenizer=tokenizer, aggregation_strategy="simple", device="cuda:0")
    print('------------------------------ After Creating Huggingface ner pipeline ------------------', flush=True)

    print('------------------------------ Begin Creating Huggingface SequenceTagger ------------------', flush=True)
    tagger = SequenceTagger.load("flair/chunk-english").to('cuda')
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
