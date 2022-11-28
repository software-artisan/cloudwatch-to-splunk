#!/usr/bin/env python3

try:
    from time import time
    import boto3
    import os
    import sys
    import datetime
    import tzlocal
    import argparse
    print('Hello World', flush=True)
except Exception as e1:
    print('Caught ' + str(e1), flush=True)
os._exit(os.EX_OK)
