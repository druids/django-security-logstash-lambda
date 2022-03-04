import base64
import json
import os
import zlib
import socket
import boto3

from uuid import uuid4

try:
    import json
except ImportError:
    import simplejson as json

# Parameters

logstash = False
sqs = False

if os.environ.get('LOGSTASH', 'off') == 'on':
    logstash = True
    host = os.environ['LOGSTASH_HOST']
    port = int(os.environ['LOGSTASH_PORT'])
    timeout = int(os.environ.get('LOGSTASH_TIMEOUT', 5))


if os.environ.get('SQS', 'off') == 'on':
    sqs = True
    queue_name = os.environ['SQS_QUEUE']


def serialize_message(message, metadata):
    return json.dumps({
        'message': message,
        'meta': metadata,
    })


def lambda_handler(event, context):
    # Add the context to meta
    metadata = {
        'aws': {
            'function_name': context.function_name,
            'function_version': context.function_version,
            'invoked_function_arn': context.invoked_function_arn,
            'memory_limit_in_mb': context.memory_limit_in_mb,
        }
    }
    # Parse logs
    logs = awslogs_handler(event)

    if logstash:
        send_to_logstash(logs, metadata)

    if sqs:
        send_to_sqs(logs, metadata)


def send_to_logstash(logs, metadata):
    if not host or not port:
        raise Exception(
            'You must configure your Logstash hostname and port before '
            'starting this lambda function (see #Parameters section)'
        )

    socket.setdefaulttimeout(timeout)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        for log_message, log_metadata in logs:
            s.send(bytes(serialize_message(log_message, merge_dicts(log_metadata, metadata) + '\n'), 'utf-8'))
    finally:
        s.close()


def send_to_sqs(logs, metadata):
    if not queue_name:
        raise Exception(
            'You must configure your SQS queue name before '
            'starting this lambda function (see #Parameters section)'
        )

    sqs_client = boto3.client('sqs')

    queue_url = sqs_client.get_queue_url(
        QueueName=queue_name,
    )['QueueUrl']

    for log_message, log_metadata in logs:
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=serialize_message(log_message, merge_dicts(log_metadata, metadata)),
            MessageGroupId=str(uuid4()),
            MessageDeduplicationId=str(uuid4()),
        )


# Handle CloudWatch events and logs
def awslogs_handler(event):
    data = zlib.decompress(base64.b64decode(event['awslogs']['data']), 16 + zlib.MAX_WBITS)
    logs = json.loads(data.decode('utf-8'))
    return [
        (log['message'], {
            'aws': {
                'log_group': logs['logGroup'],
                'log_stream': logs['logStream'],
                'owner': logs['owner'],
                'log_event_id': log['id'],
                'log_timestamp': log['timestamp'],
            }
        }) for log in logs['logEvents']
    ]


def merge_dicts(a, b, path=None):
    path = [] if path is None else path

    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dicts(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise Exception(
                        'Conflict while merging metadatas and the log entry at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a
