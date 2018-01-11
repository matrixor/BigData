import boto3
import json
from datetime import datetime
import time
import kinesis_helper as kh
import multiprocessing

class Kinesis_Consumer():
    my_stream_name      = 'wmi-kinesis-demo-1'
    region              = 'us-east-1'
    kinesis_helper      = kh.Kinesis_Helper()
    kinesis_client      = kinesis_helper.getKinesisClient(region)
    batch_size          = 2
    shardIteratorType   = 'LATEST' ##Allowed Values: AT_SEQUENCE_NUMBER | AFTER_SEQUENCE_NUMBER | TRIM_HORIZON | LATEST
    
    def printRecords(self,recordsResponse):
        if len(recordsResponse['Records'])> 0:
              for res in recordsResponse['Records']: 
                print('res: ' + str(res))
    
    def getBatchRecords(self, shard_iterator):
        record_response = self.kinesis_client.get_records(ShardIterator=shard_iterator, 
                                                          Limit=self.batch_size)
        #print('record_response: ' + str(record_response))
        self.printRecords(record_response)
        return record_response
    
    def getRecords(self,shard_iterator):
        record_response = self.getBatchRecords(shard_iterator)
        
        while 'NextShardIterator' in record_response:
            record_response = self.getBatchRecords(record_response['NextShardIterator'])
            time.sleep(5)

        #return record_response
                
    def consumeStream(self):
        my_stream_describe  = self.kinesis_helper.getStreamDescribe(self.kinesis_client, self.my_stream_name)
        shards              = my_stream_describe['StreamDescription']['Shards']
        
        #my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
        jobs = []
        for shard in shards:
            my_shard_id = shard['ShardId']
            print('start shard_id: ' + my_shard_id)
            shard_iterator = self.kinesis_client.get_shard_iterator(StreamName=self.my_stream_name,
                                                                    ShardId=my_shard_id,
                                                                    ShardIteratorType=self.shardIteratorType)
            my_shard_iterator = shard_iterator['ShardIterator']
            process = multiprocessing.Process(target=self.getRecords, args=(my_shard_iterator,))
            process.start()
            jobs.append(process)
        
        for job in jobs:
            job.join()

            
if __name__ == '__main__':
    kc = Kinesis_Consumer()
    kc.consumeStream()