import os
import boto3


session = boto3.session.Session()
client = session.client(
    "s3",
    region_name="sfo2",
    endpoint_url=os.environ["DIGITALOCEAN_API_URL"],
    aws_access_key_id=os.environ["ACCESS_KEY"],
    aws_secret_access_key=os.environ["SECRET_KEY"],
)

target = os.environ["TO_UPLOAD"]
contents = os.listdir(target)
total = len(contents)
for index, filename in enumerate(contents):
    client.upload_file(
        f"{os.path.join(target, filename)}",
        "rioiconography",
        f"situatedviews/{filename}",
        ExtraArgs={"ACL": "public-read", "ContentType": "image/jpeg"},
    )
    print(f"{filename} uploaded successfully ({index+1}/{total})")
