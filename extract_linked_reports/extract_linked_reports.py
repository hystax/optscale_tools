#!/usr/bin/env python3

# -------------------------------------------------------------------
# Copyright 2016-2020 Hystax Inc
# All Rights Reserved
#
# NOTICE:  Hystax permits you to use this file in accordance
# with the terms of the license agreement
# accompanying it.  If you have received this file from a source
# other than Hystax, then your use, modification, or distribution
# of it requires the prior written permission of Hystax.
# -------------------------------------------------------------------

import argparse
import csv
import logging
import os
import tempfile
import zipfile

import boto3

LOG = logging.getLogger(__name__)


def str_to_set(input_str):
    result = {s.strip() for s in input_str.split(',')}
    result = {s for s in result if s}
    return result


def find_reports(bucket, path_prefix):
    items = bucket.objects.filter(Prefix=path_prefix)
    reports = {}
    for item in items:
        relative_path = item.key[len(path_prefix):]
        if relative_path.endswith('.csv.zip'):
            reports[relative_path] = item
    return reports


def extract_expenses(source_report_path, target_report_path, usage_account_ids):
    with open(source_report_path, newline='') as source_f:
        with open(target_report_path, 'w') as target_f:
            reader = csv.DictReader(source_f)
            writer = csv.DictWriter(target_f, reader.fieldnames)
            source_lines = 0
            target_lines = 0
            writer.writeheader()
            for row in reader:
                source_lines += 1
                if row['lineItem/UsageAccountId'] in usage_account_ids:
                    writer.writerow(row)
                    target_lines += 1
            LOG.info('Found {} lines, removed {} lines, left {} lines'.format(
                source_lines, source_lines - target_lines, target_lines))


def process_report(source_bucket, source_path, source_report_name,
                   target_bucket, target_path, target_report_name,
                   usage_account_ids):
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = os.path.join(temp_dir, 'report.zip')
        target_report_path = os.path.join(temp_dir, 'new_report.csv')

        source_bucket.download_file(source_path, zip_path)

        with zipfile.ZipFile(zip_path, 'r') as f:
            if len(f.filelist) > 1:
                raise Exception('Too many files, expected one file')
            f.extractall(temp_dir)
            source_report_path = os.path.join(
                temp_dir, f.filelist[0].filename)

        extract_expenses(source_report_path, target_report_path,
                         usage_account_ids)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as f:
            target_report_file = os.path.basename(source_report_path).replace(
                source_report_name, target_report_name, 1)
            f.write(target_report_path, target_report_file)

        target_bucket.upload_file(zip_path, target_path)


def main(source_bucket_name, source_report_path_prefix, source_report_name,
         target_bucket_name, target_report_path_prefix, target_report_name,
         usage_account_ids, source_access_key_id, source_secret_access_key,
         target_access_key_id, target_secret_access_key):
    source_session = boto3.Session(
        aws_access_key_id=source_access_key_id,
        aws_secret_access_key=source_secret_access_key)
    target_session = boto3.Session(
        aws_access_key_id=target_access_key_id,
        aws_secret_access_key=target_secret_access_key)
    source_s3 = source_session.resource('s3')
    target_s3 = target_session.resource('s3')

    source_bucket = source_s3.Bucket(source_bucket_name)
    target_bucket = target_s3.Bucket(target_bucket_name)
    source_prefix = '{}/{}/'.format(source_report_path_prefix,
                                    source_report_name)
    target_prefix = '{}/{}/'.format(target_report_path_prefix,
                                    target_report_name)

    source_reports = find_reports(source_bucket, source_prefix)
    target_reports = find_reports(target_bucket, target_prefix)

    LOG.info('Found {} source reports and {} target reports'.format(
        len(source_reports), len(target_reports)))

    for source_relative_path, source_report in source_reports.items():
        target_relative_path = source_relative_path.replace(
            source_report_name, target_report_name, 1)
        target_report = target_reports.get(target_relative_path)
        LOG.info('Checking report {}'.format(source_relative_path))
        if (not target_report or target_report.last_modified <
                source_report.last_modified):
            LOG.info('Processing report {}'.format(source_relative_path))
            source_path = source_report.key
            target_path = target_prefix + target_relative_path
            process_report(
                source_bucket, source_path, source_report_name, target_bucket,
                target_path, target_report_name, usage_account_ids)
            LOG.info('Saved processed report as {}'.format(
                target_relative_path))
        else:
            LOG.info('Report is already processed as {}'.format(
                target_relative_path))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description='Script to extract linked account billing report data')

    parser.add_argument('--source_bucket_name', type=str, required=True,
                        help='Source bucket name')
    parser.add_argument('--source_report_path_prefix', type=str, required=True,
                        help='Source report path prefix')
    parser.add_argument('--source_report_name', type=str, required=True,
                        help='Source report name')

    parser.add_argument('--target_bucket_name', type=str, required=True,
                        help='Target bucket name')
    parser.add_argument('--target_report_path_prefix', type=str, required=True,
                        help='Target report path prefix')
    parser.add_argument('--target_report_name', type=str, required=True,
                        help='Target report name')

    parser.add_argument('--usage_account_ids', type=str_to_set, required=True,
                        help='AWS usage account IDs to include into target '
                             'report (comma-separated)')

    parser.add_argument('--source_access_key_id', type=str, required=True,
                        help='Source AWS account access key ID')
    parser.add_argument('--source_secret_access_key', type=str, required=True,
                        help='Source AWS account secret access key')

    parser.add_argument('--target_access_key_id', type=str, required=True,
                        help='Target AWS account access key ID')
    parser.add_argument('--target_secret_access_key', type=str, required=True,
                        help='Target AWS account secret access key')

    arguments = parser.parse_args()
    main(**vars(arguments))
