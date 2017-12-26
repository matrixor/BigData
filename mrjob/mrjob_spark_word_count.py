### You must define this file name as 'mrjob_spark_xxxxx'
### xxxxx is your job description, like 'word_count'
#########################################################

from mrjob.job import MRJob

class MRSparkJob(MRJob):
    
    ### update below 'job' function to define your job 
    def job(self,lines):
        from operator import add
        import re
        
        WORD_RE = re.compile(r"[\w']+")
        
        counts = (
            lines.flatMap(lambda line: WORD_RE.findall(line))
            .map(lambda word: (word, 1))
            .reduceByKey(add))
        
        return counts    
    
    
    def spark(self, input_path, output_path):
        
        from pyspark import SparkContext
        
        sc = SparkContext(appName='wmi_mr_spark_job')
        input_lines = sc.textFile(input_path)
        results = self.job(input_lines)
        results.saveAsTextFile(output_path)   
        sc.stop()

    
if __name__ == '__main__':
    MRSparkJob.run()