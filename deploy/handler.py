import urllib.parse
import boto3
import pickle
import base64

def sprite_s3(event, context):
    client = boto3.client('s3')

    # download and unpickle data chunk from s3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    obj = client.get_object(Bucket=bucket, Key=key)
    pickled_chunk = obj['Body'].read()
    chunk = pickle.loads(pickled_chunk)

    # get pickled function
    obj = client.get_object(Bucket="vinn-dump", Key="pickled_function")
    pickled_function = obj['Body'].read()
    my_func = pickle.loads(pickled_function)

    # apply function to chunk and store results
    res = my_func(chunk)
    pickled_obj = pickle.dumps(res)
    client.put_object(Body=pickled_obj, Bucket="vinn-dump", Key=key)

    # qa
    print(chunk)
    print(my_func.__name__)
    print(res)

    return True


def sprite_event(event: Dict, context: Any):
    function_bytes = event['function'].encode('utf8')
    function_pkl = base64.decodebytes(function_bytes)
    function = pickle.loads(function_pkl)
    return function()
