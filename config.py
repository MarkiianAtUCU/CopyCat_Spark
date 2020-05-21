AWS_CREDENTIALS = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
}

SPARK_MASTER_URI = "spark:/<ip>:7077"
SPARK_CLUSTER = [
    ('spark.executor.memory', '2500m'),
    ('spark.executor.cores', '2'),
]

S3_DATA_LOCATION = "s3a://mmarkiian.s3/copycat_inc/data/"
S3_OUTPUT_BUCKET = "mmarkiian.s3"
