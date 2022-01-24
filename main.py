import boto3
from pprint import pprint as pp
import json
import pandas as pd
import numpy as np

# import pandas as pd
# import io
#
# df = pd.DataFrame([[1,2,3,4,5], [10,20,30,40,50]])
# str_buffer = io.StringIO()
# df.to_csv(str_buffer)
# s3_client.put_object(
#     Body=str_buffer.getvalue(),
#     Bucket=bucket_name,
#     Key='Test/data.csv'
# )
#
# # Or using resource:
# s3_resource = boto3.resource('s3')
# s3_resource.Object(
#     bucket_name,
#     'Test/data.csv'
# ).put(
# Body=str_buffer.getvalue()
# )

# bucket = s3_resource.Bucket(bucket_name)

# bucket_list = s3_client.list_buckets()

#
# for objects in bucket_contents["Contents"]:
#     print(objects['Key'])

# contents = bucket.objects.all()

# for object in contents:
#     print(object.key)

# s3_object = s3_client.get_object(Bucket = bucket_name, Key = 'python/chatbot-intent.json')
# strbody = s3_object['Body'].read()
# pp(json.loads(strbody))

# s3_object = s3_client.get_object(Bucket = bucket_name, Key = 'python/happiness-2019.csv')
# df = pd.read_csv(s3_object['Body'])
# pp(df)

# dict_to_upload = {'name': 'data', 'status': 1}
#
# with open('SandroD.json', 'w') as jsonfile:
#     json.dump(dict_to_upload, jsonfile)
#
# s3_client.upload_file(Filename = 'SandroD.json', Bucket = bucket_name, Key = 'Data26/Test/SandroD.json')


class Fish_market:

    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = 'data-eng-resources'

        self.bucket_contents = self.s3_client.list_objects_v2(Bucket=self.bucket_name)

        self.processing_files = []

        files = self.read_from_s3()
        df = self.get_df(files)
        df = self.calculate_avg(df)
        self.out_csv(df)
        self.upload()

    def read_from_s3(self):

        self.processing_files = []
        for file in self.bucket_contents["Contents"]:
            if file['Key'].startswith("python/fish-market"):
                self.processing_files.append(file['Key'])

        return self.processing_files

    def get_df(self, files):
        cols = pd.read_csv(self.s3_client.get_object(Bucket=self.bucket_name, Key=files[0])['Body']).columns
        df = pd.DataFrame(columns=cols)
        for file in files:
            s3_object = pd.read_csv(self.s3_client.get_object(Bucket=self.bucket_name, Key= file)['Body'])
            df = pd.concat([df, s3_object], axis = 0)
        df = df.reset_index()
        df = df.drop('index', axis=1)
        return df

    def calculate_avg(self, df):

        return df.groupby('Species').mean()

    def out_csv(self, df):
        return df.to_csv("Sandro-fish-market.csv")

    def upload(self):

        self.s3_client.upload_file(Filename = 'Sandro-fish-market.csv', Bucket = self.bucket_name, Key = 'Data26/fish/Sandro-fish-market.csv')

        print('Document successfully uploaded')

f = Fish_market()