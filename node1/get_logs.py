#!/usr/bin/env python3

from time import time
import boto3
import os
import sys
import datetime
import tzlocal


for a in sys.argv:
    print(str(a), flush=True)

os._exit(os.EX_OK)
