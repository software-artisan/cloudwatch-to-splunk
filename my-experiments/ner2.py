#!/usr/bin/env python3
try:
    from time import time
    import boto3
    import os
    import sys
    from datetime import datetime, timezone, timedelta
    import tzlocal
    import argparse
    from concurrent_plugin import concurrent_core
    from transformers import AutoTokenizer, pipeline
    from optimum.intel.openvino import OVModelForTokenClassification
    import json
    import urllib
    from urllib.parse import quote
    from infinstor import infin_boto3
    import torch
    from pynvml import *
    import sqlparse

    #modelname='alexjercan/codebert-base-buggy-token-classification'
    #modelname = 'mrm8488/codebert-base-finetuned-stackoverflow-ner'
    modelname = 'Jean-Baptiste/roberta-large-ner-english'
    #modelname = 'kevinjesse/codeberta-MT4TS'
    #modelname = 'sagorsarker/codeswitch-hineng-lid-lince'

    #ctext = 'Using customerId c175fb2c-9a2e-47fa-81ef-12a13a453919 for searching Buckets table'
    #ctext = 'Using jagane c175fb2c-9a2e-47fa-81ef-12a13a453919 for searching Buckets table'
    ctext = "update actor set first_name = 'HARPO' where first_name = 'GROUCHO' and last_name = 'WILLIAMS'"

    print('------------------------------ Begin Loading Huggingface ner model ' + modelname + ' ------------------', flush=True)
    try:
        tokenizer = AutoTokenizer.from_pretrained(modelname)
        model = OVModelForTokenClassification.from_pretrained(modelname, from_transformers=True)
    except Exception as err:
        print('Caught ' + str(err) + ' while loading ner model ' + modelname)
    print('------------------------------ After Loading Huggingface ner model ------------------', flush=True)

    print('------------------------------ Begin Creating Huggingface ner pipeline ------------------', flush=True)
    ner = pipeline('ner', model=model, tokenizer=tokenizer, aggregation_strategy="simple")
    print('------------------------------ After Creating Huggingface ner pipeline ------------------', flush=True)

    before = datetime.now()
    print('Classification text =' + ctext, flush=True)
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
