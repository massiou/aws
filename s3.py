import boto
from boto.s3.connection import S3Connection
from boto.iam.connection import IAMConnection
from boto.exception import BotoServerError
from boto.exception import S3CreateError

def connec_to_iam(access_key, secret_key):
    """
    :param access_key: IAM access key
    :param secret_key: IAM secret key
    :return:
    """
    conn_iam = IAMConnection("", "")
    return conn_iam

def connect_to_s3(access_key, secret_key):
    """
    :param access_key: IAM access key
    :param secret_key: IAM secret key
    :return:
    """
    conn_s3 = S3Connection("", "")
    return conn_s3


def create_s3_user(user_name):
    """
    :param user_name: S3 username
    :return: None
    """
    try:
        conn_iam.create_user(user_name, '/')
    except BotoServerError as exc:
        print '[Exception] {}'.format(exc.message)


def create_s3_bucket(bucket_name):
    """
    :param bucket_name: S3 bucket name
    :return: None
    """
    try:
        conn_s3.create_bucket(bucket_name=bucket_name,
                              headers=None,
                              location=boto.s3.connection.Location.EU,
                              policy=None)
    except S3CreateError as exc:
        print '[Exception] {}'.format(exc.message)


def delete_s3_user(user_name):
    """
    :param user_name: S3 username
    :return:
    """
    try:
        conn_iam.delete_user(user_name)
    except BotoServerError as exc:
        print '[Exception] {}'.format(exc.message)


if __name__ == '__main__':
    pass

