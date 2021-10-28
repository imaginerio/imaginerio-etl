import os

import dagster as dg
from dotenv import load_dotenv
from IIIFpres.iiifpapi3 import *
from IIIFpres.utilities import *

load_dotenv(override=True)


class S3IOManager(dg.IOManager):
    def __init__(
        self,
        s3_bucket,
        s3_session,
        s3_prefix=None,
    ):
        self.bucket = dg.check.str_param(s3_bucket, "s3_bucket")
        # self.s3_prefix = dg.check.str_param(s3_prefix, "s3_prefix")
        self.s3 = s3_session
        # self.s3.head_bucket(Bucket=self.bucket)

    def load_input(self, context):
        name = context.upstream_output.name
        content = context.upstream_output.metadata["content"]
        key = context.upstream_output.metadata["storage"] + name
        # context.log.debug(f"Loading S3 object from: {self._uri_for_key(key)}")
        obj = self.s3.get_object(
            Bucket=self.bucket,
            Key=key,
            ResponseContentType=content,
        )

        return obj

    def handle_output(self, context, obj):
        if obj:
            # obj = [{"data":data, "key": path, "type": json},{"data":data, "key": path, "type": json},{"data": path, "key":path,"type": image},]
            for item in obj:
                if item["type"] == "json":
                    print(self.bucket, item["key"])
                    self.s3.put_object(
                        Bucket=self.bucket,
                        Body=item["data"],
                        Key=item["key"],
                        ContentType="application/json",
                    )

                elif item["type"] == "path":
                    # context.log.debug("Upload image at: {0}".format(item["data"]))
                    for root, dirs, files in os.walk(item["data"]):
                        for file in files:
                            key = os.path.join(root, file)
                            content = (
                                "image/jpeg"
                                if file.endswith("jpg")
                                else "application/json"
                            )
                            try:
                                print(
                                    key,
                                    self.bucket,
                                    key,
                                    content,
                                )
                                self.s3.upload_file(
                                    key,
                                    self.bucket,
                                    key,
                                    ExtraArgs={"ContentType": content},
                                )
                            except Exception as e:
                                print(
                                    "Couldn't upload image {0}.Error: {1}".format(
                                        key, e
                                    )
                                )

        # content = context.metadata["content"]
        # name = context.name
        # key = context.metadata["storage"] + name

        # if isinstance(obj, str) and len(obj) < 40:  # if path
        #     upload_start = time.time()
        #     context.log.debug(f"Uploading the file at: {self._uri_for_key(key)}")
        #     self.s3.upload_file(
        #         obj,
        #         self.bucket,
        #         key,
        #         ExtraArgs={"ContentType": content},
        #     )
        #     upload_end = time.time()
        #     print("Upload: ", upload_end - upload_start)

        # elif isinstance(obj, str):  # if json
        #     context.log.debug(f"Writing json object at: {self._uri_for_key(key)}")
        #     self.s3.put_object(
        #         Bucket=self.bucket,
        #         Body=obj,
        #         Key=key,
        #         ContentType=content,
        #     )
        # elif isinstance(obj, list):
        #     for item in obj:

        # else:  # if image
        #     writing_start = time.time()
        #     context.log.debug(f"Upload the object at: {self._uri_for_key(key)}")
        #     img_bytes = io.BytesIO()
        #     # obj.save(img_bytes, format="jpg")
        #     # img_bytes = bytearray(obj)
        #     self.s3.upload_fileobj(
        #         img_bytes, self.bucket, key, ExtraArgs={"ContentType": content}
        #     )
        #     writing_end = time.time()
        #     print("Wrinting: ", writing_end - writing_start)


@dg.io_manager(
    config_schema={
        "s3_bucket": dg.StringSource,
        # "s3_prefix": dg.Field(
        #     dg.StringSource, is_required=False, default_value="dagster"
        # ),
    },
    required_resource_keys={"s3"},
)
def s3_io_manager(init_context):
    s3_session = init_context.resources.s3
    s3_bucket = init_context.resource_config["s3_bucket"]
    return S3IOManager(s3_bucket, s3_session)
