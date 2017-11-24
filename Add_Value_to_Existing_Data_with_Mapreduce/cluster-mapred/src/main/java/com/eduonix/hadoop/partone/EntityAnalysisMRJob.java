package com.eduonix.hadoop.partone;

import java.io.* ;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EntityAnalysisMRJob extends Configured implements Tool {

    private static final String projectRootPath = System.getProperty("user.dir");
    public static final  boolean runOnCluster = true; // local run need set false
    
    public static final String hdfsClusterPath = "/user/alan/study/cluster-mapred/"; // if runOnCluster = true, HDFS path

    private static final String END_CLUSTER_FLAG = "END_CLUSTER_FLAG";
    private static final String START_CLUSTER_FLAG = "START_CLUSTER_FLAG";

    private static final String raw_data = "ComercialBanks10k.csv";
    private static final String mapped_data = "output";
    private static final String mappedDataForAnalysis = "/mapped_data";

    private static Path outputFile;
    private static Path inputFile;
    private static Path mappedDataPath;
    private static Configuration conf;

    public static void main(String[] args) throws Exception
    {
        // this main function will call run method defined below.
        int res = ToolRunner.run(new Configuration(), new EntityAnalysisMRJob(), args);
        // res : 0 - job success ; 1 - job failure
        if(res==0  && runOnCluster) {
        	// copy input & output file from hdfs to local
            runMigrate();
        }

        System.exit(res);
    }



    public int run(String[] strings) throws Exception {
    	conf = getConf();

        File localOutputDirectory = new File(String.format("%s%s",projectRootPath ,"/output" ));
        if(localOutputDirectory.exists()) {
            System.out.println("Mapreduce output folder exists in local filesystem  deleting run local test again");
            delete(localOutputDirectory);
            return 1;
        }

        File localMappedDirectory = new File(String.format("%s%s",projectRootPath ,mappedDataForAnalysis ));
        if(localMappedDirectory.exists()) {
            System.out.println("Mapped data output folder exists in local filesystem  deleting run local test again");
            delete(localMappedDirectory);
            return 1;
        }

        outputFile = new Path(projectRootPath, mapped_data);
        inputFile = new Path(projectRootPath, raw_data);
        Job job = Job.getInstance(conf);

        job.setJarByClass(EntityAnalysisMRJob.class);
        job.setMapperClass(EntityMapper.class);
        
        //job.setReducerClass(EntityReducer.class);
        job.setReducerClass(EntityReducerClusterSeeds.class);

        // these values define the types for the MAGIC shuffle sort steps
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static int runMigrate( ) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path( projectRootPath);      
        mappedDataPath = new Path( tmpPath.toString()+mappedDataForAnalysis);

        System.out.println( String.format("  mappedDataPath %s", mappedDataPath ));
        // projectRootPath /home/user/hou/zhanga1/workspace/bigdata/study/cluster-mapred/uberjar
        // mappedDataPath /home/user/hou/zhanga1/workspace/bigdata/study/cluster-mapred/uberjar/mapped_data

        // Here is copy to local. It can change to copy to hdfs.
        fs.copyToLocalFile(false, outputFile, mappedDataPath);
        fs.copyToLocalFile(false, inputFile, mappedDataPath);

        return 0;
    }

    public static class EntityMapper  extends Mapper<Object, Text, Text, Text>
    {
        private final static Text valueLine = new Text();
        private Text keyWord = new Text();
        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {

            String[] line = value.toString().split("\\t") ;
            if(line[0].equals("ID")) {
              return;
            }

            // set the name of the bank as our key
            keyWord.set(line[1]);
            valueLine.set(value);
            // emitt
            context.write(keyWord, valueLine);
        }
    }

    /*  Sample out put:
     * 
     *  ABERDEEN ASSET MANAGEMENT	1
		ABERDEEN ASSET MANAGERS LIMITED, DEUTSCHLAND BRANCH	1
		ABIOMED, INC.	1
		Found Duplicate Values for ABN AMRO
			2
		ABN AMRO ASSET MANAGEMENT (USA) LLC	1
		ABN AMRO Clearing Bank NV	1
		ABN AMRO Clearing Chicago LLC	1
		ABN AMRO Pensioenfonds	1
		ABN Amro Clearing Ltd Hong Kong	1
     * 
     * */
    public static class EntityReducer  extends Reducer<Text,Text,Text,IntWritable> {

        public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
            int total = 0;
            for (Text val : values) {
                total++ ;
            }
            if( total > 1)  {
                // found a possible duplicate
                StringBuilder logger = new StringBuilder();
                logger.append("Found Duplicate Values for ").append(key).append("\n");

                for (Text val : values) {
                    logger.append(val).append("\n");
                }
                context.write( new Text(logger.toString()), new IntWritable(total));

            } else {
                context.write(key, new IntWritable(total));
            }
        }
    }

    /*  Sample out put:
     * 
		START_CLUSTER_FLAG
		ABN AMRO	0013000000Eq2aDAAR	ABN AMRO	499 Washington Blvd Ste 400	Jersey City	NJ	07310-1995	US	123458	
		ABN AMRO	001a000001Qq67RAAR	ABN AMRO	Gustav Mahlerlaan 10	Amsterdam 1082PP			Netherlands	-13546594	
			END_CLUSTER_FLAG
		START_CLUSTER_FLAG
		AMVESCAP	0013000000Fgan7AAB	AMVESCAP	1360 Peachtree St NE	Atlanta	GA	30309-3283	US	(404) 479-1095	
		AMVESCAP	0013000000Qblz1AAB	AMVESCAP	1315 Peachtree St NE	Atlanta	GA	30309-7515	US	(404) 479 - 1095	
			END_CLUSTER_FLAG
		START_CLUSTER_FLAG
		ANZ	0013000000ZDKKHAA5	ANZ	833 collins street	melbourne	victoria	vic3000	australia		
		ANZ	0013000000fxcSzAAI	ANZ		Singapore			Singapore		
			END_CLUSTER_FLAG
		START_CLUSTER_FLAG
		ARC Resources Ltd.	0013000000R8BHQAA3	ARC Resources Ltd.	2100, 440 - 2ND AVENUE S.W.	Calgary	AB	T2P 5E9	Canada		
		ARC Resources Ltd.	0013000000Eq2wVAAR	ARC Resources Ltd.	2100, 440 - 2ND AVENUE S.W.	CALGARY	AB	T2P 5E9	CANADA	(403) 503-8600	
			END_CLUSTER_FLAG
		START_CLUSTER_FLAG
		AXA Advisors, LLC	0013000000QblqyAAB	AXA Advisors, LLC	1290 Avenue of the Americas Fl CONC1	New York	NY	10104-1472	US	(212) 314-4600	
		AXA Advisors, LLC	0013000000hwZxMAAU	AXA Advisors, LLC	1290 Avenue of the Americas Fl CONC1	New York	NY	10104-1472			
		AXA Advisors, LLC	0013000000Fgan5AAB	AXA Advisors, LLC	1290 Avenue of the Americas Fl CONC1	New York	NY	10104-1472	US	(212) 314-4600	
			END_CLUSTER_FLAG
     * 
     * */
    public static class EntityReducerClusterSeeds  extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

            int total = 0;
            StringBuilder logger = new StringBuilder();
            logger.append(START_CLUSTER_FLAG).append("\n");
            for (Text val : values) {
                total++ ;
                logger.append(key).append("\t").append(val).append("\n");
            }
            if( total > 1)  {
                context.write(new Text(logger.toString()), new Text(END_CLUSTER_FLAG));
            }
        }
    }

    public static void delete(File file) throws IOException{

        if(file.isDirectory()){
            //directory is empty, then delete it
            if(file.list().length==0){
                file.delete();
                System.out.println("Directory is deleted : " + file.getAbsolutePath());
            }else{
                //list all the directory contents
                String files[] = file.list();
                for (String temp : files) {
                    //construct the file structure
                    File fileDelete = new File(file, temp);
                    //recursive delete
                    delete(fileDelete);
                }
                //check the directory again, if empty then delete it
                if(file.list().length==0){
                    file.delete();
                    System.out.println("Directory is deleted : " + file.getAbsolutePath());
                }
            }
        }else{
            //if file, then delete it
            file.delete();
            System.out.println("File is deleted : " + file.getAbsolutePath());
        }
    }
}


