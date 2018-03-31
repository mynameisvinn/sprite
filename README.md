# sprite
sprite is an mapreduce implementation with lambda. 

with sprite, you can write single threaded code and still get distributed systems performance, with zero provisioning or cluster management.

## why serverless?
classic mapreduce with hadoop/spark/dask means touching infrastructure code. with Sprite, we dont have to think about server provisioning, cluster management, resource management, scheduling. in fact, we wont need to know *anything* about infrastructure.

## example
```python
v = Sprite(bucket="mynameisvinn", access="a", secret="s")
v.map(np.sum, np.arange(100), n_chunks=10)
# returns [45, 145, 245, 345, 445, 545, 645, 745, 845, 945]
```