
import boto3
from pyspark.rdd import RDD


class S3BytesRDDUploader:
    def __init__(self,Bucket,Key,*data):
        self.bucket = Bucket
        self.key = Key
        self.uploaded = False

        try:
            r = boto3.client('s3').head_bucket(Bucket=Bucket)
        except Exception as e:
            raise RuntimeError(f"Cannot find Bucket {Bucket}")

        if data:
            self.data = validate_data(data)
        else:
            self.data = list()

    def add(self,*data):
        if self.uploaded:
            raise RuntimeError(f"Data already uploaded to S3")
        self.data.extend(validate_data(data))

    def upload(self):
        if self.uploaded:
            raise RuntimeError(f"Data already uploaded to S3")
        if not self.data:
            raise RuntimeError(f"No data to upload to S3")

        self.data = self._consolidate_bytes()
        self._partition_for_s3()

        self.uploaded = True


    def _consolidate_bytes(self):
        rval = list()
        bstr = b""
        for item in self.data:
            if type(item) is bytes:
                bstr += item
            else:
                if bstr:
                    prepender = BytePrepender(bstr)
                    item = item.zipWithIndex().map(prepender)
                    bstr = b""
                rval.append(item)
        
        if not rval:
            raise RuntimeError("Data must contain at least one RDD")

        if bstr:
            appender = ByteAppender(bstr,rval[-1].count())
            rval[-1] = rval[-1].zipWithIndex().map(appender)

        return rval

    def _partition_for_s3(self):
        rdd = self.data[0]
        for x in self.data[1:]:
            rdd = rdd.union(x)

        lengths = rdd.map(lambda x:len(x)).collect()
        
        partitioner = S3Partitioner()
        parts = [partitioner(x) for x in lengths]

        rdd = rdd.zipWithIndex().map(lambda x:(x[0],(parts[x[1]],x[1])))
        self.data = rdd


class BytePrepender:
    def __init__(self,bstr):
        self.bstr = bstr
    def __call__(self,x):
        return self.bstr + x[0] if x[1]==0 else x[0]

class ByteAppender:
    def __init__(self,bstr,n):
        self.bstr = bstr
        self.n = n
    def __call__(self,x):
        return x[0] + self.bstr if x[1]==self.n-1 else x[0]

class S3Partitioner:
    def __init__(self):
        self.cur = (0,0)
    def __call__(self,x):
        size = x + self.cur[0]
        rval = self.cur[1]
        self.cur = (0,rval+1) if size > 5*1024*1024 else (size,rval)
        return rval

def validate_data(data_list):
    """Validates data to be added to an S3BytesRDDUploader

    Args:
        data (list): List of items to be appended to the S3 file

    Returns:
        data (list): Completely flattened version of the input data list

    Raises: TypeError if list contains anything other than valid items.

    Valid items include:

        - bytestrings
        - RDDs containing nothing but bytestrings
        - lists of valid items (and may be nested)
    """

    rval = list()
    for item in data_list:
        if type(item) is list:
            rval.extend( validate_data(item) )
        elif type(item) is bytes:
            rval.append(item)
        elif type(item) is RDD:
            if type(item.cache().first()) is not bytes:
                raise TypeError(
                    f"RDDs may only contain bytestring elements, not {type(item.first())}")
            rval.append(item)
        else:
            raise TypeError(
                f"Data must be either bytes or RDD of bytes, not {type(item)}")
    return rval

