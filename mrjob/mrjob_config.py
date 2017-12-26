from datetime import datetime

runner         = 'emr'
region         = 'us-east-1'
clusterId      = ''
bucketName     = 'ivz_wmi_mrjob'
cloudTmpDir    = 's3://' + bucketName + '/tmp/'
inputFileName  = 'sample_word_count.txt'
inputDir       = '/test-input/'
inputFile      =  's3://' + bucketName + inputDir + inputFileName

jobName        = 'noname_job'
outputDir      = 's3://' + bucketName + '/output/' + jobName \
                 + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

args_base      = [
       '-r', runner,
       '--region',region,
       #'--cluster-id',clusterId,
       '--cloud-tmp-dir',cloudTmpDir,
       '--no-output', 
       '--output-dir',outputDir,        
       inputFile
    ]