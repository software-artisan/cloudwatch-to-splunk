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
    from transformers import pipeline
    from transformers import AutoTokenizer, AutoModelForTokenClassification
    import json
    import urllib
    from urllib.parse import quote
    from infinstor import infin_boto3
    import torch
    from pynvml import *
    import sqlparse

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

    output_list = ner([sys.argv[1]])
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
