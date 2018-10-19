#!/usr/bin/env python3
# coding: utf-8

"""
Perform a S3 bucket snapshot
"""

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from datetime import timezone
import threading
import time

import boto3

ACCESS_KEY = 'XXX'
SECRET_KEY = 'XXX'


def list_bucket(client, bucket):
    """
    Bucket listing

    :param client: s3 client object
    :type client: boto3.client()
    :param bucket: bucket name
    :return: list of objects
    :rtype: list
    """
    keys = []

    # Create a paginator to browse all objects
    paginator = client.get_paginator('list_objects')

    pages = paginator.paginate(
        Bucket=bucket,
    )

    # Loop on all pages
    for request in pages:
        c_keys = request.get('Contents', [])
        keys.extend([key['Key'] for key in c_keys])

    return keys


def date_to_ts(date):
    """
    Convert date into timestamp

    :param date: date to be converted
    :type date: datetime.datetime()
    :rtype: integer
    """
    return date.replace(tzinfo=timezone.utc).timestamp()


def get_versions(client, bucket, timestamp):
    """
    Retrieve all versions from a bucket until a timestamp specified

    :param client: s3 client object
    :type client: boto3.client()
    :param bucket: bucket name
    :type bucket: string
    :param timestamp: time boundary
    :type timestamp: integer
    :return: list of versions
    :rtype: list
    """
    versions = []

    # Create a paginator to browse all versions
    paginator = client.get_paginator('list_object_versions')

    pages = paginator.paginate(
        Bucket=bucket,
    )

    # Loop on all pages
    for request in pages:
        c_versions = request.get('Versions', [])
        versions.extend(c_versions)

    # Only returns the one's created after `timestamp`
    return [v for v in versions if date_to_ts(v['LastModified']) < timestamp]


def copy_object(client, copy_source, bucket, object_name):
    """

    :param copy_source:
    :param bucket:
    :param object_name:
    :return:
    """
    return client.copy_object(
        CopySource=copy_source, Bucket=bucket, Key=object_name
    )


def copy_objects(client, bucket_src, bucket_dest, timestamp):
    """
    Copy the more recent version (<timestamp) of all objects from source to dest

    :param client: s3 client object
    :type client: boto3.client()
    :param bucket_src: source bucket name
    :type bucket_src: string
    :param bucket_dest: destination bucket name
    :type bucket_dest: string
    :param timestamp: time boundary
    :type timestamp: integer
    :return: list of s3 objects
    :rtype: list
    """
    objects_to_copy = []

    all_versions = get_versions(client, bucket_src, timestamp)

    # Unique objects name
    objects = set([obj['Key'] for obj in all_versions])

    for object_name in objects:
        # Build all the versions for `object_name`
        versions = [
            (object_name, obj.get('VersionId', 0), obj.get('LastModified'))
            for obj in all_versions if obj['Key'] == object_name
        ]

        # Get only the more recent version
        version_to_copy = max(versions, key=lambda x: x[2])

        copy_source = {
            'Bucket': bucket_src,
            'Key': object_name,
            'VersionId': str(version_to_copy[1])
        }

        objects_to_copy.append((copy_source, object_name))

    # Multithread the copies
    threads = []

    start = time.time()
    for obj in objects_to_copy:
        copy_source, obj_name = obj
        thr = threading.Thread(
            target=copy_object, args=(
                client, copy_source, bucket_dest, obj_name
            )
        )
        thr.start()
        threads.append(thr)

    # Wait for all threads to terminate
    for thr in threads:
        thr.join()

    duration = time.time() - start
    print("Copy {0} objects in {1} s".format(len(objects_to_copy), duration))
    return objects_to_copy


def arg_parse():
    """
    Parse script arguments

    """
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument(
        '-s', '--bucket_source',
        help='bucket source name',
        required=True)

    parser.add_argument(
        '-d', '--bucket_dest',
        help='bucket destination name',
        required=True)

    parser.add_argument(
        '-t', '--timestamp',
        help='timestamp limit',
        type=int,
        required=True)

    parser.add_argument(
        '-e', '--endpoint',
        help='http endpoint',
        required=True)

    args = parser.parse_args()

    return parser, args


def main():
    """
    Entry point
    """
    _, args = arg_parse()

    client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=args.endpoint,
    )

    ret = copy_objects(client, args.bucket_source, args.bucket_dest, args.timestamp)
    print(ret)


if __name__ == '__main__':
    main()
