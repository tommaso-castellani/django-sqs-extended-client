import base64
import json
import os
import tempfile
import uuid
from enum import Enum
from io import BytesIO

import boto3
from boto3.session import Session


class SQSExtendedClientConstants(Enum):
    DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144
    MAX_ALLOWED_ATTRIBUTES = 10 - 1  # 10 for SQS, 1 for the reserved attribute
    RESERVED_ATTRIBUTE_NAME = "SQSLargePayloadSize"
    S3_BUCKET_NAME_MARKER = "-..s3BucketName..-"
    S3_KEY_MARKER = "-..s3Key..-"


class SNSClientExtended(object):
    """
    A session stores configuration state and allows you to create service
    clients and resources.
    :type aws_access_key_id: string
    :param aws_access_key_id: AWS access key ID
    :type aws_secret_access_key: string
    :param aws_secret_access_key: AWS secret access key
    :type aws_region_name: string
    :param aws_region_name: AWS region name
    :type s3_bucket_name: string
    :param s3_bucket_name: S3 bucket name

    """

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, 
                 aws_region_name=None, s3_bucket_name=None, always_through_s3=False):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region_name = aws_region_name
        self.s3_bucket_name = s3_bucket_name
        self.message_size_threshold = SQSExtendedClientConstants.DEFAULT_MESSAGE_SIZE_THRESHOLD.value
        self.always_through_s3 = always_through_s3
        if aws_access_key_id and aws_secret_access_key and aws_region_name:
            self.sns = boto3.client(
                'sns',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=aws_region_name
            )
            self.sqs = boto3.client(
                'sqs',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=aws_region_name
            )
        else:
            self.sns = boto3.client('sns')
            self.sqs = boto3.client('sqs')

    @staticmethod
    def is_large_payload_support_enabled():
        return True

    def set_always_through_s3(self, always_through_s3):
        """
    Sets whether or not all messages regardless of their payload size should be stored in Amazon S3.
    """
        self.always_through_s3 = always_through_s3

    def set_message_size_threshold(self, message_size_threshold):
        """
    Sets the message size threshold for storing message payloads in Amazon S3.
    Default: 256KB.
    """
        self.message_size_threshold = message_size_threshold

    @staticmethod
    def __get_string_size_in_bytes(message_body):
        return len(message_body.encode('utf-8'))

    @staticmethod
    def __stringToBase64(s):
        return base64.b64encode(s.encode('utf-8'))

    @staticmethod
    def __base64ToString(b):
        return base64.b64decode(b).decode('utf-8')

    def __is_base64(self, s):
        try:
            encoded = self.__stringToBase64(self.__base64ToString(s))
            return encoded == str.encode(s)
        except Exception as e:
            return False

    def __get_msg_attributes_size(self, message_attributes):
        # sqs binaryValue expects a base64 encoded string as all messages in sqs are strings
        total_msg_attributes_size = 0

        for key, entry in message_attributes.items():
            total_msg_attributes_size += self.__get_string_size_in_bytes(key)
            if entry.get('DataType'):
                total_msg_attributes_size += self.__get_string_size_in_bytes(entry.get('DataType'))
            if entry.get('StringValue'):
                total_msg_attributes_size += self.__get_string_size_in_bytes(entry.get('StringValue'))
            if entry.get('BinaryValue'):
                if self.__is_base64(entry.get('BinaryValue')):
                    total_msg_attributes_size += len(str.encode(entry.get('BinaryValue')))
                else:
                    total_msg_attributes_size += self.__get_string_size_in_bytes(entry.get('BinaryValue'))

        return total_msg_attributes_size

    def __is_large(self, message, message_attributes):
        msg_attributes_size = self.__get_msg_attributes_size(message_attributes)
        msg_body_size = self.__get_string_size_in_bytes(message)
        total_msg_size = msg_attributes_size + msg_body_size
        return total_msg_size > self.message_size_threshold

    def receive_message(self, queue_url, max_number_of_messages=1, wait_time_seconds=10):
        """
        Retrieves one or more messages (up to 10), from the specified queue. Using the WaitTimeSeconds parameter enables long-poll support
            The message body.
            An MD5 digest of the message body. For information about MD5, see RFC1321 .
            The MessageId you received when you sent the message to the queue.
            The receipt handle.
            The message attributes.
            An MD5 digest of the message attributes.
            The receipt handle is the identifier you must provide when deleting the message
        """
        response_opt_queue = self.sqs.receive_message(QueueUrl=queue_url, AttributeNames=['All'],
                                                      MessageAttributeNames=['All', ],
                                                      MaxNumberOfMessages=max_number_of_messages,
                                                      WaitTimeSeconds=wait_time_seconds, )
        opt_messages = response_opt_queue.get('Messages', [])
        if not opt_messages:
            return None
        for message in opt_messages:
            message['Body'] = json.loads(message.get('Body'))
            large_pay_load_attribute_value = message['Body'].get('MessageAttributes', {}).get(
                SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value, None)
            if large_pay_load_attribute_value:
                try:
                    message_body_inner = json.loads(message['Body'].get('Message'))
                    if 's3BucketName' not in message_body_inner and 's3Key' not in message_body_inner:
                        raise ValueError('Detected missing required key attribute s3BucketName and s3Key in s3 payload')
                    s3_bucket_name = message_body_inner.get('s3BucketName')
                    s3_key = message_body_inner.get('s3Key')
                    orig_msg_body = self.get_text_from_s3(s3_bucket_name, s3_key)
                    message['Body']['Message'] = orig_msg_body
                    message['Body']['MessageAttributes']['s3_key'] = s3_key
                    message['Body']['MessageAttributes']['s3_bucket_name'] = s3_bucket_name
                    # remove the additional attribute before returning the message to user.
                    message['Body'].get('MessageAttributes').pop(
                        SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value)
                    # Embed s3 object pointer in the receipt handle.
                    modified_receipt_handle = SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER.value + s3_bucket_name + SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER.value + SQSExtendedClientConstants.S3_KEY_MARKER.value + s3_key + SQSExtendedClientConstants.S3_KEY_MARKER.value + message.get(
                        'ReceiptHandle')
                    message['ReceiptHandle'] = modified_receipt_handle
                except ValueError:
                    raise ValueError('Decoding JSON has failed')
            else:
                message['Body'] = json.loads(message.get('Body')) if isinstance(message.get('Body'), str) else message.get('Body')

        return opt_messages

    def __delete_message_payload_from_s3(self, receipt_handle, flush_s3):
        try:
            s3_msg_bucket_name = self.__get_bucket_marker_from_receipt_handle(receipt_handle,
                                                                              SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER.value)
            s3_msg_key = self.__get_bucket_marker_from_receipt_handle(receipt_handle,
                                                                      SQSExtendedClientConstants.S3_KEY_MARKER.value)
            session = Session(aws_access_key_id=self.aws_access_key_id,
                              aws_secret_access_key=self.aws_secret_access_key, region_name=self.aws_region_name)
            s3 = session.resource('s3')
            s3_object = s3.Object(s3_msg_bucket_name, s3_msg_key)
            if flush_s3:
                s3_object.delete()
                print('Deleted s3 object https://s3.amazonaws.com/{}/{}'.format(s3_msg_bucket_name, s3_msg_key))
        except Exception as e:
            print("Failed to delete the message content in S3 object. {}, type:{}".format(
                str(e), type(e).__name__))
            raise e

    @staticmethod
    def __get_bucket_marker_from_receipt_handle(receipt_handle, marker):
        start_marker = receipt_handle.index(marker) + len(marker)
        end_marker = receipt_handle.rindex(marker, start_marker)
        return receipt_handle[start_marker:end_marker]

    @staticmethod
    def __get_orig_receipt_handle(receipt_handle):
        return receipt_handle[receipt_handle.rindex(SQSExtendedClientConstants.S3_KEY_MARKER.value) + len(
            SQSExtendedClientConstants.S3_KEY_MARKER.value):]

    @staticmethod
    def __is_s3_receipt_handle(receipt_handle):
        return True if SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER.value in receipt_handle and SQSExtendedClientConstants.S3_KEY_MARKER.value in receipt_handle else False

    def delete_message(self, queue_url, receipt_handle, flush_s3):
        """
        Deletes the specified message from the specified queue. You specify the message
        by using the message's receipt handle and not the MessageId you receive when you
        send the message. Even if the message is locked by another reader due to the
        visibility timeout setting, it is still deleted from the queue. If you leave
        a message in the queue for longer than the queue's configured retention period,
        Amazon SQS automatically deletes the message.

        Additionally to purging the queue of the message any s3 referenced object will be deleted
        """
        if self.__is_s3_receipt_handle(receipt_handle):
            self.__delete_message_payload_from_s3(receipt_handle, flush_s3)
            receipt_handle = self.__get_orig_receipt_handle(receipt_handle)
        print("receipt_handle={}".format(receipt_handle))
        try:
            self.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        except Exception:
            #  silence exception as it happens randomly when AWS deleted it automatically.
            pass

    def send_message(self, topic, message, message_attributes: dict, message_deduplication_id, message_group_id):
        """
        Delivers a message to the specified queue and uploads the message payload
        to Amazon S3 if necessary.
        """
        if message is None:
            raise ValueError('message_body required')

        if isinstance(message, (dict, list)):
            message = json.dumps(message)

        msg_attributes_size = self.__get_msg_attributes_size(message_attributes)
        if msg_attributes_size > self.message_size_threshold:
            raise ValueError(
                "Total size of Message attributes is {} bytes which is larger than the threshold of {} Bytes. Consider including the payload in the message body instead of message attributes.".format(
                    msg_attributes_size, self.message_size_threshold))

        message_attributes_number = len(message_attributes)
        if message_attributes_number > SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES.value:
            raise ValueError(
                "Number of message attributes [{}}] exceeds the maximum allowed for large-payload messages [{}].".format(
                    message_attributes_number, SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES.value))

        large_payload_attribute_value = message_attributes.get(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value)
        if large_payload_attribute_value:
            raise ValueError("Message attribute name {} is reserved for use by SQS extended client.".format(
                SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value))

        kwargs = {'TopicArn': topic, 'MessageAttributes': message_attributes, 'Message': message}

        if message_group_id:
            kwargs['MessageGroupId'] = message_group_id

        if message_deduplication_id:
            kwargs['MessageDeduplicationId'] = message_deduplication_id

        if self.always_through_s3 or self.__is_large(str(message), message_attributes):
            if not self.s3_bucket_name.strip():
                raise ValueError('S3 bucket name cannot be null')
            s3_key_message = json.dumps(self.__store_message_in_s3(message))
            message_attributes[SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value] = {
                'StringValue': str(self.__get_string_size_in_bytes(message)), 'DataType': 'Number'}
            kwargs['Message'] = s3_key_message

        return self.sns.publish(**kwargs)

    def __store_message_in_s3(self, message_body):
        """
        Store SQS message body into user defined s3 bucket
        prerequisite aws credentials should have access to write to defined s3 bucket
        """
        try:
            s3_key = str(uuid.uuid4())
            session = Session(aws_access_key_id=self.aws_access_key_id,
                              aws_secret_access_key=self.aws_secret_access_key, region_name=self.aws_region_name)
            s3 = session.resource('s3')
            opt_file = tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8', delete=False)
            opt_file.write(str(message_body))
            opt_file.flush()
            reader = open(opt_file.name, mode='r', encoding='utf-8')
            s3.Bucket(self.s3_bucket_name).put_object(Key=s3_key, Body=reader.read())
            reader.close()
            opt_file.close()
            if os.path.exists(opt_file.name):
                os.remove(opt_file.name)
            return {'s3BucketName': self.s3_bucket_name, 's3Key': s3_key}
        except Exception as e:
            print("Failed to store the message content in an S3 object. SQS message was not sent. {}, type:{}".format(
                str(e), type(e).__name__))
            raise e

    def get_text_from_s3(self, s3_bucket_name, s3_key):
        """
        Get string representation of a sqs object and store into original SQS message object
        """
        session = Session(aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key,
                          region_name=self.aws_region_name)
        s3 = session.resource('s3')
        bucket = s3.Bucket(s3_bucket_name)
        objs = list(bucket.objects.filter(Prefix=s3_key))
        if objs and objs[0].key == s3_key:
            data_byte_io = BytesIO()
            bucket = s3.Bucket(s3_bucket_name)
            bucket.Object(s3_key).download_fileobj(data_byte_io)
            data_byte_io.seek(0)
            return data_byte_io.read().decode('utf-8')
        return None
