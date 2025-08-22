import json, os, boto3, botocore
from datetime import datetime

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "local")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "local")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
BUCKET = os.getenv("S3_BUCKET", "sports")

s3 = boto3.resource(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    endpoint_url=S3_ENDPOINT_URL,
)

try:
    s3.meta.client.head_bucket(Bucket=BUCKET)
except botocore.exceptions.ClientError:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Created bucket s3://{BUCKET}")

key = f"bronze/smoke_test/dt={datetime.utcnow().date()}/hello.json"
obj = s3.Object(BUCKET, key)
payload = {"msg": "hello, sporty!", "ts_utc": datetime.utcnow().isoformat()}
obj.put(Body=json.dumps(payload).encode("utf-8"))
print(f"Wrote s3://{BUCKET}/{key}")

# 3) read it back
body = s3.Object(BUCKET, key).get()["Body"].read()
print("Read back:", body.decode("utf-8"))

# 4) list the prefix
print("Objects under bronze/smoke_test/:")
for o in s3.Bucket(BUCKET).objects.filter(Prefix="bronze/smoke_test/"):
    print(" -", o.key)
