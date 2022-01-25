import boto3
import pandas as pd


class FishMarket:

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


f = FishMarket()