import kinesis_helper as kh
import kinesis_producer as kp
import kinesis_consumer as kc

import time

steamName        = 'wmi-kinesis-demo-1'
shards           = 3

def main():
    kinesis_helper   = kh.Kinesis_Helper()
    kinesis_producer = kp.Kinesis_Producer()
    kinesis_consumer = kc.Kinesis_Consumer()
    
    streams          = kinesis_helper.listStreams()
    #print(streams);
    isSteamNameExist = False
    
    for stream_name in streams['StreamNames']:
        if stream_name == steamName:
            print(steamName + ' exist!')
            isSteamNameExist = True
            
    if not isSteamNameExist:
        print(steamName + ' creating ......')
        kinesis_helper.createStream(steamName, shards)
        # wait for stream created
        time.sleep(60)   
        
    # demo produce stream data
    #kinesis_producer.get_fake_data_put_to_stream()
    
    # demo consume stream data
    #kinesis_consumer.consumeStream()


if __name__ == '__main__':
    main()