import os
import boto3

session = boto3.session.Session()
client = session.client(
    "s3",
    region_name="sfo2",
    endpoint_url="https://sfo2.digitaloceanspaces.com",
    aws_access_key_id=os.environ["ACCESS_KEY"],
    aws_secret_access_key=os.environ["SECRET_KEY"],
)


for filename in os.listdir(target):
    client.upload_file(
        f"{os.path.join(target, filename)}",
        "rioiconography",
        f"situatedviews/{filename}",
        ExtraArgs={"ACL": "public-read", "ContentType": "image/jpeg"},
    )

