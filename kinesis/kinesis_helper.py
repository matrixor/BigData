from boto import kinesis
import boto3

class Kinesis_Helper():
    region  = 'us-east-1'
    kinesis = kinesis.connect_to_region(region)
        
    def createStream(self,steamName, shards):
        ## try catch exception
        self.kinesis.create_stream(steamName, shards)
        
    def listStreams(self):
        streams = self.kinesis.list_streams()
        return streams
    
    def getStreamDescribe(self,kinesisClient,steamName):
        response = kinesisClient.describe_stream(StreamName=steamName)
        return response
    
    def deleteStream(self,steamName):
        pass
    
    def getKinesisClient(self,region):
        kinesis_client = boto3.client('kinesis', region_name=region)
        return kinesis_client