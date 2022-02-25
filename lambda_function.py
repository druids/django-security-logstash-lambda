import base64
import json
import zlib
import socket

try:
    import json
except ImportError:
    import simplejson as json


# Parameters
host = ''
port = ''

metadata = {}


def serialize_message(message, metadata):
    return bytes(
        json.dumps({
            'message': message,
            'meta': metadata,
        }) + '\n', 'utf-8'
    )


def lambda_handler(event, context):
    if not host or not port:
        raise Exception(
            'You must configure your Logstash hostname and port before '
            'starting this lambda function (see #Parameters section)'
        )

    # Attach Logstash TCP Socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    try:
        # Add the context to meta
        metadata['aws'] = {}
        metadata['aws']['function_name'] = context.function_name
        metadata['aws']['function_version'] = context.function_version
        metadata['aws']['invoked_function_arn'] = context.invoked_function_arn
        metadata['aws']['memory_limit_in_mb'] = context.memory_limit_in_mb

        # Parse logs
        logs = awslogs_handler(event)

        for message, metadata in logs:
            s.send(serialize_message(message, metadata))
    finally:
        s.close()


# Handle CloudWatch events and logs
def awslogs_handler(event):
    # Get logs
    data = zlib.decompress(base64.b64decode(event['awslogs']['data']), 16 + zlib.MAX_WBITS)
    logs = json.loads(data.decode('utf-8'))

    structured_logs = []

    # Send lines to Logstash
    for log in logs['logEvents']:
        structured_logs.append((
            log['message'], {
                'aws': {
                    'log_group': logs['logGroup'],
                    'log_stream': logs['logStream'],
                    'owner': logs['owner'],
                    'log_event_id': log['id'],
                    'log_timestamp': log['timestamp'],
                }
            }
        ))

    return structured_logs


def merge_dicts(a, b):
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dicts(a[key], b[key], [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise Exception(
                        'Conflict while merging metadatas and the log entry at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a
