import os
from iptcinfo3 import IPTCInfo
import exiftool
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

JPEG_HD = os.environ["JPEG_HD"]

df = pd.read_csv(
    "src/data-out/metadata.csv",
    dtype=str,
    encoding="utf-8",
    parse_dates=["date", "start_date", "end_date"],
)
df.fillna(value="", inplace=True)
df.set_index("id", inplace=True)

files = [
    os.path.join(JPEG_HD, file)
    for file in os.listdir(JPEG_HD)
    if not os.path.exists(os.path.join(JPEG_HD, f"{file}_original"))
]

for i, item in enumerate(files):
    if item.endswith(".jpg"):
        basename = os.path.split(item)[1]
        name = basename.split(".")[0]
        try:
            if df.loc[name, "date_accuracy"] == "circa":
                datecreated = (
                    df.loc[name, "start_date"].strftime("%Y")
                    + "/"
                    + df.loc[name, "end_date"].strftime("%Y")
                )
            elif df.loc[name, "date_accuracy"] == "year":
                datecreated = df.loc[name, "date"].strftime("%Y")
            elif df.loc[name, "date_accuracy"] == "month":
                datecreated = df.loc[name, "date"].strftime("%Y-%m")
            elif df.loc[name, "date_accuracy"] == "day":
                datecreated = df.loc[name, "date"].strftime("%Y-%m-%d")
        except AttributeError:
            print(f"Review {basename} date")
        byline = df.loc[name, "creator"]
        headline = df.loc[name, "title"]
        caption = df.loc[name, "description"]
        objecttype = df.loc[name, "type"]
        dimensions = (
            f'{df.loc[name, "image_width"]}cm x {df.loc[name, "image_height"]}cm'
        )
        keywords = df.loc[name, "wikidata_depict"].split("||")
        latitude = df.loc[name, "lat"]
        longitude = df.loc[name, "lng"]
        altitude = df.loc[name, "height"]
        imgdirection = df.loc[name, "heading"]

        params = [
            "-IPTC:Source=Instituto Moreira Salles/IMS",
            "-IPTC:CopyrightNotice=This image is in the Public Domain.",
            "-IPTC:City=Rio de Janeiro",
            "-IPTC:Province-State=RJ",
            "-IPTC:Country-PrimaryLocationName=Brasil",
            "-GPSLatitudeRef=S",
            "-GPSLongitudeRef=W",
            "-GPSAltitudeRef=0",
            "-GPSImgDirectionRef=T",
            f"-IPTC:By-line={byline}",
            f"-IPTC:ObjectName={name}",
            f"-IPTC:Headline={headline}",
            f"-IPTC:Caption-Abstract={caption}",
            f"-IPTC:ObjectTypeReference={objecttype}",
            f"-IPTC:Keywords={keywords}",
            f"-GPSLatitude={latitude}",
            f"-GPSLongitude={longitude}",
            f"-GPSAltitude={altitude}",
            f"-GPSImgDirection={imgdirection}",
        ]
        with exiftool.ExifTool(executable_="C:\exiftool.exe") as et:
            for param in params:
                param = param.encode(encoding="utf-8")
                dest = item.encode(encoding="utf-8")
                et.execute(param, dest)
        print(
            f"{basename}\n{df.loc[name, 'date']}\nTagged {i+1} of {len(files)} images"
        )
