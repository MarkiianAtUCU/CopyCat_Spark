AWS_CREDENTIALS = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
}

SPARK_MASTER_URI = ""
SPARK_CLUSTER = [
    ('spark.executor.memory', '8g'),
    ('spark.executor.cores', '60'),
    ('spark.cores.max', '6'),
    ('spark.driver.memory', '8g')
]

S3_DATA_LOCATION = "s3a://mmarkiian.s3/copycat_inc/data/"
S3_OUTPUT_BUCKET = "mmarkiian.s3"
