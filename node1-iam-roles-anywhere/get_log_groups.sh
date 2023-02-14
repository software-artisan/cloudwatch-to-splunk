#!/bin/bash

set -x
python ./get_log_groups.py --groupname_startswith /aws/lambda/concurrent-1-0-10-2022101-S3CustomResourceAWSLambd-vbfUK1RJnDAI --aws_profile_iam_roles_anywhere subscriber_infinlogs_iam_roles_anywhere