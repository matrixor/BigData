### Run command : python mrjob_runner mrjob_spark_xxx
### mrjob_spark_xxx is the job moudle name, which file name is mrjob_spark_xxx.py

import sys
import mrjob_config as cfg

def main():
    jobModuleName  = sys.argv[1]
    jobName        = jobModuleName.split('mrjob_spark_')[1]
    
    MyJob  = __import__(jobModuleName)
    
    cfg.outputDir  = cfg.outputDir.replace(cfg.jobName, jobName)
    args_runtime   = [arg.replace(cfg.jobName,jobName) for arg in cfg.args_base]
    cfg.jobName    = jobName

    mr_job = MyJob.MRSparkJob(args=args_runtime)   
    runner = mr_job.make_runner()
    result = runner._launch()


if __name__ == '__main__':
    main()