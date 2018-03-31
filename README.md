# sprite
sprite is an mapreduce implementation with aws lambda. inspired by [david hayden](https://davidhayden.org/articles/create-python-function-on-aws-lambda-using-serverless/).

## why serverless?
classic mapreduce with hadoop/spark/dask means touching infrastructure code. 

with lambdas, we dont have to think about server provisioning, cluster management, resource management, scheduling. in fact, we wont need to know *anything* about infrastructure.