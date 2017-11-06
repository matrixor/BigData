## Clustering Example

- In many clustering applications the first step is to find the equivalent entities and build the clusters from these   
  hear we can exploit the fact that map-reduce aggregates the identical keys, by selecting the key as the match entity  
  we can create initial seed clusters, then we an apply a metric to aggregate the remaining data to these centers  
  
  
  
    100% equivalent              node1             node2 .....  
    90%  equivalent   subnode 1a      subnode 1a ............                 
    repeat
    
    
- To run code
 * 1 run com.eduonix.hadoop.partone.EntityAnalysisMRJob to produce the traning data, set    
   `public static final  boolean runOnCluster = false` for local testing on Linux  
    `public static final  boolean runOnCluster = true` for Hadoop  
 * 2 run  com.eduonix.hadoop.partone.etl.EntityAnalysisETL  
 
 
## AZ
 Build
 	mvn clean install -Pmapreduce
 	-- it will generate uberjsr/ubu-mr.jar and dependency-reduced-pom.xml  
 	mvn clean install -Petl
 	-- it will generate uberjsr/ubu-etl.jar
 
 Run
 	Copy ComercialBanks.csv to ${project.basedir}/uberjar/
 	
	-- local
 	java -jar ubu-mr.jar
 	
 	-- hadoop cluster
 	hadoop jar ubu-mr.jar
 	
 	-- it will generate 'output' folder under ${project.basedir}/uberjar/, including two files : 'part-r-00000' , '_SUCCESS'
 	
 	