
import boto3
from pyspark.rdd import RDD

class S3BytesRDDUploader:
    """
    This class aggregates a mixture of bytestrings (bytes) and RDDs containing
    bytestrings and uploads the resulting bytestring to S3.

    At least one of the elements to be updated must be an RDD as the actual S3
    upload will occur on Spark executor nodes.

    A S3BytesRDDUploader is contructed with the following Args:

        - Bucket (string): The S3 bucket (top level directory) name
        - Key (string): The s3 filename (includes full path within the bucket)
        - data (list): Optional list of bytestrings and RDDs to be updloaded

        Data may be added during or after contruction (or both).  Data will
        be written to the S3 file in the order they are added.

    Raises: TypeError if anyting other than bytestrings or RDDs of bytestrings
    are included in the input data list.

    """
    def __init__(self,Bucket,Key,*data):
        self.bucket = Bucket
        self.key = Key
        self.uploadId = None

        try:
            r = boto3.client("s3").head_bucket(Bucket=Bucket)
        except Exception as e:
            raise RuntimeError(f"Cannot find Bucket {Bucket}")

        if data:
            self.data = self.validate_data(data)
        else:
            self.data = list()

    def __del__(self):
        if self.uploadId:
            # If create_multipart_upload was called (and succesful),
            #   attempt cleanup of partial uploads
            # (If complete_multipart_upload was successful, this will do nothing)
            s3.abort_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.uploadId)


    def add(self,*data):
        """
        Adds additional bytestrings and/or RDDs to the data to be uploaded to S3

        Args:
            data (list): list of bytestrings or RDDs of bytestrings to be added

        Data will be written to the S3 file in the order they are added.

        Raises: 

            - RuntimeError if data has already been uploaded to S3

            - TypeError if anyting other than bytestrings or RDDs of bytestrings
            are included in the data list.

        """
        if self.uploadId:
            raise RuntimeError(f"Data already uploaded to S3 {self.uploadId}")

        self.data.extend(self.validate_data(data))


    def upload(self):
        """
        Performs the actual upload of data to S3 using the multiplart upload
        function.

        Args:
            n/a

        Returns:
            n/a

        Raises: RuntimeError if upload fails or if data has already be uploaded
        """
        if self.uploadId:
            raise RuntimeError(f"Data already uploaded to S3 {self.uploadId}")
        if not self.data:
            raise RuntimeError(f"No data to upload to S3")

        # convert bytestrings into RDDs 
        #  insert into proper place in the list of bytestring RDDs

        rdds = list()

        sc = None
        for item in self.data:
            if type(item) is RDD:
                sc = item.context
                break

        if not sc:
            raise RuntimeError("Data must contain at least one RDD")

        bstr = list()
        for item in self.data:
            if type(item) is bytes:
                bstr.append(item)
            else:
                if bstr:
                    rdds.append( sc.parallelize(bstr) )
                    bstr = list()
                rdds.append(item)

        if bstr:
            rdds.append( sc.parallelize(bstr) )

        # merge all RDDs into a single RDD
        rdd = rdds[0]
        for r in rdds[1:]:
            rdd = rdd.union(r)

        # partition for S3

        lengths = rdd.map(lambda x:len(x)).collect()

        partitioner = S3Partitioner()
        parts = [partitioner(x) for x in lengths]

        rdd = (rdd
               .zipWithIndex()
               .map(lambda x:(x[0],(parts[x[1]],x[1])))
               .groupBy(lambda x:x[1][0])
               .map(lambda x:(x[0],b"".join(t[0] for t in x[1])))
              )

        # initiate upload

        s3 = boto3.client("s3")
        mpu = s3.create_multipart_upload(Bucket=self.bucket, Key=self.key)

        self.uploadId = mpu["UploadId"]

        # perform upload (in Spark executor nodes)

        uploader = S3PartUploader(mpu)
        result = rdd.map(uploader).collect()

        # verify that upload was succesful, if not raise an exception

        if all(r["success"] for r in result):
            parts = [{"PartNumber":i+1, "ETag":r["ETag"]} 
                     for i,r in enumerate(result) ]

            s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                MultipartUpload={"Parts":parts},
                UploadId=self.uploadId)
        else:
            error = ", ".join( r["error"] for r in result if not r["success"] )
            raise RuntimeError(
                f"Failed to upload s3://{self.bucket}/{self.key}\n   {error}")


    @classmethod
    def validate_data(cls,data_list):
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
                rval.extend( cls.validate_data(item) )
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


class S3Partitioner:
    """This callable class assigns S3 Part numbers for a multipart upload so as 
    to ensure all parts (with possible exception of last part) have at least 5MB 
    of data in them.

    The assigned part number will be based on a running history of all the
    element lengths past so far.

    Args:
        length (int): size (bytes) of element to add to the S3 upload

    Returns:
        part (int): part index to assign to the element

    """
    def __init__(self):
        self.cur = (0,0)
    def __call__(self,x):
        size = x + self.cur[0]
        rval = self.cur[1]
        self.cur = (0,rval+1) if size > 5*1024*1024 else (size,rval)
        return rval


class S3PartUploader:
    """This callable class uploads a single part in and S3 multipart upload.

    It is constructed with a single argument, the dictionary returned by 
    create_multipart_upload.

    Args:
        indexed_data (tuple): 

            index (int): the part index to upload
            body (bytes): the body of the part to upload

    Returns:
        result (dict):
            success (bool): flag indicating success of the upload
            ETag (string): returned on success
            error (string): returned on failure
    """
    def __init__(self,mpu):
        self.bucket = mpu["Bucket"]
        self.key = mpu["Key"]
        self.uploadId = mpu["UploadId"]
    def __call__(self,x):
        s3 = boto3.client("s3")
        try:
            part = s3.upload_part(
                Bucket = self.bucket,
                Key = self.key,
                PartNumber = 1 + x[0],
                UploadId = self.uploadId,
                Body = x[1])
        except Exception as e:
            return {"success":False, "error":f"Failed to load part {1+x[0]}"}
        
        return {"success":True, "ETag":part["ETag"]}

        

