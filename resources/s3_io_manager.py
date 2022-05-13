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

    def handle_output(self, path):
        if os.path.isdir(path):
            for root, _, files in os.walk(path):
                for file in files:
                    key = os.path.join(root, file)
                    content = (
                        "image/jpeg" if file.endswith("jpg") else "application/json"
                    )
                    print("Would be uploading {0} as {1}".format(key, content))
                    # self.s3.upload_file(
                    #     key,
                    #     self.bucket,
                    #     key,
                    #     ExtraArgs={"ContentType": content},
                    # )
        else:
            content = "image/jpeg" if path.endswith("jpg") else "application/json"
            print("Would be uploading {0} as {1}".format(path, content))
            # self.s3.upload_file(
            #     path,
            #     self.bucket,
            #     path,
            #     ExtraArgs={"ContentType": content},
            # )
            os.remove(path)

    # def handle_output(self, context, obj):
    #     if obj:
    #         # obj = [{"data":data, "key": path, "type": json},{"data":data, "key": path, "type": json},{"data": path, "key":path,"type": image},]
    #         for item in obj:
    #             if item["type"] == "json":
    #                 print(self.bucket, item["key"])
    #                 self.s3.put_object(
    #                     Bucket=self.bucket,
    #                     Body=item["data"],
    #                     Key=item["key"],
    #                     ContentType="application/json",
    #                 )

    #             elif item["type"] == "path":
    #                 # context.log.debug("Upload image at: {0}".format(item["data"]))
    #                 for root, dirs, files in os.walk(item["data"]):
    #                     for file in files:
    #                         key = os.path.join(root, file)
    #                         content = (
    #                             "image/jpeg"
    #                             if file.endswith("jpg")
    #                             else "application/json"
    #                         )
    #                         try:
    #                             print(
    #                                 key,
    #                                 self.bucket,
    #                                 key,
    #                                 content,
    #                             )
    #                             self.s3.upload_file(
    #                                 key,
    #                                 self.bucket,
    #                                 key,
    #                                 ExtraArgs={"ContentType": content},
    #                             )
    #                         except Exception as e:
    #                             print(
    #                                 "Couldn't upload image {0}.Error: {1}".format(
    #                                     key, e
    #                                 )
    #                             )
    #                         os.remove(key)


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
