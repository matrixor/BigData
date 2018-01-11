import boto3
import json
from datetime import datetime
import calendar
import random
import time

class Kinesis_Producer(): 
    my_stream_name = 'wmi-kinesis-demo-1'
    region         = 'us-east-1'
    kinesis_client = boto3.client('kinesis', region_name=region)    
    
    def generate_fake_data(self):
        property_value = random.randint(40, 120)
        property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
        thing_id = 'aa-bb'
            
        payload = {
            'prop': str(property_value),
            'timestamp': str(property_timestamp),
            'thing_id': thing_id
        }
        
        return payload
    
    # Implement this function for get real stream data in your project
    def get_stream_data(self): 
        pass
    
    def put_to_stream(self,data):
        put_response = self.kinesis_client.put_record(
                            StreamName=self.my_stream_name,
                            Data=json.dumps(data),
                            PartitionKey=data['thing_id'])
        
    def get_fake_data_put_to_stream(self):
        i = 1
        #while True:
        while i < 10:
            i = i + 1
            fakedata = self.generate_fake_data()
            #streamdata = get_stream_data()
            print(fakedata)
            
            self.put_to_stream(fakedata)
            time.sleep(5)
            
if __name__ == '__main__':
    kp = Kinesis_Producer()
    kp.get_fake_data_put_to_stream()        