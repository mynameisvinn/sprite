import numpy as np
import pickle
import boto3
import time
from concurrent.futures import ThreadPoolExecutor
from threading import current_thread

class Sprite(object):
    """
    parameters
    ----------
    tmp_bucket : str
        s3 bucket for intermediate results
    access : str
    secret : str
    
    attributes
    ----------
    n_threads : int
    sink : str
        s3 bucket for final results
    """
    def __init__(self, tmp_bucket, access, secret):
        self.tmp_bucket = tmp_bucket
        self.client = self.create_client(access, secret)
        self.n_threads = 10
        self.sink = "vinn-dump"
              
    def create_client(self, a, s):
        return boto3.client('s3', aws_access_key_id=a, aws_secret_access_key=s)
    
    def map(self, func, data, n_chunks):
        """
        parameters
        ----------
        func : numpy function
        data : numpy array
        n_chunks : int
        
        returns
        -------
        res : list
        """
        # place serialized function in sink
        self._put_function(func)
        
        # create and save data chunks to tmp bucket
        self._multi_process_data(data, n_chunks)
            
        # step 3: fetch results from sink
        while not self._check_sink(n_chunks):
            time.sleep(0.1)  # idle until all subresults can be found in sink
            print("waiting...")
        return self._collect(n_chunks)
            
    def _multi_process_data(self, data, n_chunks):
        """
        split array into smaller arrays, which are then put
        to s3 for processing.
        
        parameters
        ----------
        data : numpy array
        n_chunks : int
            number of sub arrays to create
        """
        chunks = np.split(data, n_chunks)
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            for i, c in enumerate(chunks):
                executor.submit(self._put_data, i, c)
                
    def _collect(self, n_chunks):
        # needs to be multithreaded
        res = []
        for i in range(n_chunks):
            fname = str(i)
            obj = self.client.get_object(Bucket=self.sink, Key=fname)
            pickled_function = obj['Body'].read()
            d = pickle.loads(pickled_function)
            res.append(d)
            self.client.delete_object(Bucket=self.sink, Key=fname)
        return res
    
    def _check_sink(self, n_chunks):
        return (len(self.client.list_objects(Bucket=self.sink)['Contents']) + 1) > n_chunks
    
    def _put_function(self, func):
        """serialize numpy function and place in s3 bucket
        """
        serialized_func = pickle.dumps(func)
        self.client.put_object(Body=serialized_func, Bucket=self.sink, Key="pickled_function")
    
    def _put_data(self, uid, chunk):
        pickled_data = pickle.dumps(chunk)
        self.client.put_object(Body=pickled_data, Bucket=self.tmp_bucket, Key=str(uid))