# sprite
sprite is an mapreduce implementation with aws lambda. inspired by [david hayden](https://davidhayden.org/articles/create-python-function-on-aws-lambda-using-serverless/).

## why serverless?
classic mapreduce with hadoop/spark/dask means touching infrastructure code. with Sprite, we dont have to think about server provisioning, cluster management, resource management, scheduling. in fact, we wont need to know *anything* about infrastructure.

## example
```python
v = Sprite("mynameisvinn", access="hello", secret="secret")
v.map(np.sum, np.arange(100), n_chunks=10)
# returns [45, 145, 245, 345, 445, 545, 645, 745, 845, 945]
```