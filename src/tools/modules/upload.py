import os
import boto3
import pandas as pd

# list geolocated items
camera = pd.read_csv("./src/metadata/camera/camera.csv")
geolocated = [f"{name}.jpg" for name in camera["name"]]

# start session
session = boto3.session.Session()
client = session.client(
    "s3",
    region_name="sfo2",
    endpoint_url=os.environ["DIGITALOCEAN_API_URL"],
    aws_access_key_id=os.environ["DIGITALOCEAN_ACCESS_KEY"],
    aws_secret_access_key=os.environ["DIGITALOCEAN_SECRET_KEY"],
)

# list items alredy on cloud
response = client.list_objects_v2(
    Bucket="rioiconography", MaxKeys=4000, Prefix="situatedviews/"
)
cloud = [item["Key"] for item in response["Contents"]]

# list items on local folder
target = os.environ["JPEG_HD"]
contents = os.listdir(target)

# filter items to be uploaded
contents = [
    x for x in contents if f"situatedviews/{x}" not in cloud and x in geolocated
]

# upload files
for index, filename in enumerate(contents):
    try:
        client.upload_file(
            f"{os.path.join(target, filename)}",
            "rioiconography",
            f"situatedviews/{filename}",
            ExtraArgs={"ACL": "public-read", "ContentType": "image/jpeg"},
        )
        print(f"{filename} uploaded successfully ({index+1}/{len(contents)})")
    except Exception as e:
        print(e)
