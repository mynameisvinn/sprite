import numpy as np
import pickle
import boto3
import time

class Sprite(object):
    def __init__(self, bucket, access, secret):
        self.bucket = bucket
        self.client = self.create_client(access, secret)
        
    def create_client(self, a, s):
        return boto3.client('s3', aws_access_key_id=a, aws_secret_access_key=s)
    
    def map(self, func, data, n_chunks):
        # serialize and put function in s3 (vinn-dumps)
        serialized_func = pickle.dumps(func)
        self.client.put_object(Body=serialized_func, Bucket="vinn-dump", Key="pickled_function")
        
        # create and save data chunks to s3 (mynameisvinn), where it's processed and saved to vinn-dump
        chunks = np.split(data, n_chunks)
        for i, c in enumerate(chunks):
            print(">>> putting", i)
            _ = self._put(i, c)
            
        # step 3: fetch results from vinn-dump
        time.sleep(3)
        res = []
        for i in range(n_chunks):
            fname = str(i)
            obj = self.client.get_object(Bucket="vinn-dump", Key=fname)
            pickled_function = obj['Body'].read()
            d = pickle.loads(pickled_function)
            res.append(d)
        
        return res
    
    def _put(self, uid, chunk):
        pickled_data = pickle.dumps(chunk)
        self.client.put_object(Body=pickled_data, Bucket=self.bucket, Key=str(uid))