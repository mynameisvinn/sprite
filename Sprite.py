import numpy as np
import pickle
import time
from concurrent.futures import ThreadPoolExecutor

import boto3


class Sprite(object):
    """
    parameters
    ----------
    data_bucket : str
        s3 bucket for intermediate results
    access : str
    secret : str

    attributes
    ----------
    n_threads : int
    function_bucket : str
        s3 bucket for final results
    """
    def __init__(
            self,
            data_bucket,
            function_bucket,
            s3 = boto3.client('s3'),
            n_threads = 10,
        ):
        self.s3 = s3
        self.data_bucket = data_bucket
        self.function_bucket = function_bucket
        self.n_threads = n_threads

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
        # place serialized function in function_bucket
        self._put_function(func)

        # create and save data chunks to tmp bucket
        self._multi_process_data(data, n_chunks)

        # step 3: fetch results from function_bucket
        while not self._check_sink(n_chunks):
            time.sleep(0.1)  # idle until all subresults can be found in function_bucket
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
            obj = self.s3.get_object(Bucket=self.function_bucket, Key=fname)
            pickled_function = obj['Body'].read()
            d = pickle.loads(pickled_function)
            res.append(d)
            self.s3.delete_object(Bucket=self.function_bucket, Key=fname)
        return res

    def _check_sink(self, n_chunks):
        return (len(self.s3.list_objects(Bucket=self.function_bucket)['Contents']) + 1) > n_chunks

    def _put_function(self, func):
        """serialize numpy function and place in s3 bucket
        """
        serialized_func = pickle.dumps(func)
        self.s3.put_object(Body=serialized_func, Bucket=self.function_bucket, Key="pickled_function")

    def _put_data(self, uid, chunk):
        pickled_data = pickle.dumps(chunk)
        self.s3.put_object(Body=pickled_data, Bucket=self.data_bucket, Key=str(uid))
