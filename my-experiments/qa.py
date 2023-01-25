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

    print('------------------------------ Begin Loading Huggingface QA pipeline ------------------', flush=True)
    try:
        from transformers import pipeline
        qa_pipeline = pipeline("question-answering", model="distilbert-base-cased-distilled-squad", revision="626af31", device=-1)
        result = qa_pipeline(question=sys.argv[1], context=sys.argv[2])
        print(str(result))
    except Exception as err:
        print('Caught ' + str(err) + ' while loading QA pipeline', flush=True)
        os._exit(os.EX_OK)
    print('------------------------------ Finished Loading Huggingface QA Pipelein ------------------', flush=True)

except Exception as e1:
    print("Caught " + str(e1), flush=True)
    os._exit(os.EX_OK)
