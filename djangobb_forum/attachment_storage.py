
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key
from djangobb_forum import settings as forum_settings

class AttachmentStorage(object):

    def __init__(self):
        self.connection = S3Connection(
            aws_access_key_id=forum_settings.EC2_ACCESS_KEY,
            aws_secret_access_key=forum_settings.EC2_SECRET_KEY,
            host=forum_settings.EC2_HOST,
            is_secure=True,
            calling_format=OrdinaryCallingFormat())
        self.bucket = self.connection.lookup(forum_settings.S3_BUCKET_NAME)
        if self.bucket is None:
            self.bucket = self.connection.create_bucket(
                forum_settings.S3_BUCKET_NAME)


    def add_attachment(self, attachment_id, attachment_fp):
        self._set_file("attachment-{}".format(attachment_id), attachment_fp)


    def get_attachment(self, attachment_id):
        key = "attachment-{}".format(attachment_id)
        try:
            return self._get(key)
        except S3ResponseError:
            return None


    def _set(self, key, value):
        k = Key(self.bucket)
        k.key = key
        k.set_contents_from_string(value)


    def _get(self, key):
        k = Key(self.bucket)
        k.key = key
        return k.get_contents_as_string()


    def _delete(self, key):
        k = Key(self.bucket)
        k.key = key
        self.bucket.delete_key(k)


    def _set_file(self, key, fp):
        k = Key(self.bucket)
        k.key = key
        k.set_contents_from_file(fp)


    def _get_file(self, key, fp):
        k = Key(self.bucket)
        k.key = key
        k.get_file(fp)
