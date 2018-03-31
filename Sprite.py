import numpy as np
import pickle
import boto3
import time
from concurrent.futures import ThreadPoolExecutor
from threading import current_thread

class Sprite(object):
    def __init__(self, bucket, access, secret):
        self.source = bucket
        self.client = self.create_client(access, secret)
        self.n_threads = 10
        self.sink = "vinn-dump"
        
    def create_client(self, a, s):
        return boto3.client('s3', aws_access_key_id=a, aws_secret_access_key=s)
    
    def process_data(self, data, n_chunks):
        chunks = np.split(data, n_chunks)
        for i, c in enumerate(chunks):
            print(">>> putting", i)
            _ = self._put(i, c)
            
    def multi_process_data(self, data, n_chunks):
        chunks = np.split(data, n_chunks)
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            for i, c in enumerate(chunks):
                _ = executor.submit(self._put, i, c)
                
    def _fetch(self, n_chunks):
        """
        should be multithreaded.
        """
        time.sleep(n_chunks/100)
        res = []
        for i in range(n_chunks):
            fname = str(i)
            obj = self.client.get_object(Bucket=self.sink, Key=fname)
            pickled_function = obj['Body'].read()
            d = pickle.loads(pickled_function)
            res.append(d)
        return res
    
    def map(self, func, data, n_chunks):
        # put serialized function in vinn-dump
        self._put_function(func)
        
        # create and save data chunks to mynameisvinn, where it's processed and saved to vinn-dump
        self.multi_process_data(data, n_chunks)
            
        # step 3: fetch results from vinn-dump
        final_results = self._fetch(n_chunks)
        
        return final_results
    
    def _put_function(self, func):
        serialized_func = pickle.dumps(func)
        self.client.put_object(Body=serialized_func, Bucket=self.sink, Key="pickled_function")
    
    def _put(self, uid, chunk):
        pickled_data = pickle.dumps(chunk)
        self.client.put_object(Body=pickled_data, Bucket=self.source, Key=str(uid))