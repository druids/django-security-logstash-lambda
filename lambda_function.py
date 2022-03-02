import base64
import json
import os
import zlib
import socket

try:
    import json
except ImportError:
    import simplejson as json

# Parameters
host = os.environ['LOGSTASH_HOST']
port = int(os.environ['LOGSTASH_PORT'])
timeout = int(os.environ.get('LOGSTASH_TIMEOUT', 5))


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
    socket.setdefaulttimeout(timeout)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    try:
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

        for log_message, log_metadata in logs:
            s.send(serialize_message(log_message, merge_dicts(metadata, log_metadata)))
    finally:
        s.close()


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
