#!/usr/bin/env python3

def print_gpu_utilization():
    nvmlInit()
    handle = nvmlDeviceGetHandleByIndex(0)
    info = nvmlDeviceGetMemoryInfo(handle)
    print(f"GPU memory occupied: {info.used//1024**2} MB.")


def print_summary(result):
    print(f"Time: {result.metrics['train_runtime']:.2f}")
    print(f"Samples/second: {result.metrics['train_samples_per_second']:.2f}")
    print_gpu_utilization()

try:
    from time import time
    import os
    import sys
    from datetime import datetime, timezone, timedelta
    import tzlocal
    import argparse
    from transformers import pipeline
    from transformers import AutoTokenizer, AutoModelForTokenClassification
    import json
    import urllib
    from urllib.parse import quote
    import torch
    from pynvml import *

    modelname = 'Jean-Baptiste/roberta-large-ner-english'

    ctext = 'Using jagane c175fb2c-9a2e-47fa-81ef-12a13a453919 for searching Buckets table'

    print('------------------------------ Begin Loading Huggingface ner model ' + modelname + ' ------------------', flush=True)
    try:
        tokenizer = AutoTokenizer.from_pretrained(modelname)
        model = AutoModelForTokenClassification.from_pretrained(modelname).to('cuda')
    except Exception as err:
        print('Caught ' + str(err) + ' while loading ner model ' + modelname)
    print('------------------------------ After Loading Huggingface ner model ------------------', flush=True)

    print_gpu_utilization()

    print('------------------------------ Begin Creating Huggingface ner pipeline ------------------', flush=True)
    ner = pipeline('ner', model=model, tokenizer=tokenizer, aggregation_strategy="simple", device="cuda:0")
    print('------------------------------ After Creating Huggingface ner pipeline ------------------', flush=True)

    print('Classification text =' + ctext, flush=True)
    before = datetime.now()
    for i in range(0, 1000):
        output_list = ner([ctext])
    after = datetime.now()
    df = after - before
    print(f"time diff={df}")
    print(str(output_list))
    for idx, one_output in enumerate(output_list):
        orgs = []
        persons = []
        misc = []
        for entry in one_output:
            print("ner ret: Entry=" + str(entry))
            s_entry_word = entry['word'].strip()
            if entry['entity_group'] == 'ORG':
                print("ner ret: Org=" + s_entry_word)
            elif entry['entity_group'] == 'PER':
                print("ner ret: person=" + s_entry_word)
            elif entry['entity_group'] == 'MISC':
                print("ner ret: entity_group=" + s_entry_word)

except Exception as e1:
    print("Caught " + str(e1), flush=True)
    os._exit(os.EX_OK)
