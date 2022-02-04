from pyspark.sql import SparkSession
from pyspark.rdd import RDD

from AWSTools import S3BytesRDDUploader

sc = SparkSession.builder.appName("S3BytesRDDUploader Test").getOrCreate().sparkContext

a = 1024*1024//10
b = 1024*1024//5
rdds = [ sc.parallelize([b'x'*(1+a*i+b*j) for j in range(10)]).cache() for i in range(5) ]

try:
    s3u = S3BytesRDDUploader('bytes-rdd-test','junk',b"123",rdds,[b"dog",b"banana"],b"cat")
except Exception as e:
    print(e)

s3u.add([b'boo',rdds[1:3],b'hiss',b' tire'])
s3u.add([b'bird',rdds[2:]])
s3u.add(b'byebye')

s3u.upload()

print("done")
