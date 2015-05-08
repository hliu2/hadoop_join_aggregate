package mapreduce;

import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
 
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.lang.*;
import java.net.URI;
import org.apache.hadoop.filecache.DistributedCache;

public class query4_1{ 
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		
		private static HashMap<String, String> cust_info = new HashMap<String, String>();
		
		public void configure(JobConf job) {
			Path[] cacheFilesLocal = new Path[0];
			try {
			    cacheFilesLocal = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException ioe){
				System.err.println("Caught exception while getting cached files: " );
			}
			
			String customerInfo =null;
			BufferedReader brReader =null;
			
			for (Path p : cacheFilesLocal) {
				try {
					brReader= new BufferedReader(new FileReader(p.toString()));
					while((customerInfo = brReader.readLine())!=null){
						String [] cust = customerInfo.split(",");
						cust_info.put(cust[0],cust[3]);
					}
				} catch (IOException ioe) {
			         System.err.println("Caught exception while parsing the cached file '"  + "' : " );
			    }
		   }
       }
	 	
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
				String [] record = value.toString().split(",");
				int ccode = Integer.parseInt(cust_info.get(record[1].toString()));
				IntWritable output_key = new IntWritable(ccode);
				Text output_value = new Text("1"+","+record[2]);
				output.collect(output_key,output_value);
		}
	}
	
	
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			 float TransTotal = 0.0f;
			 float MinTransTotal=1000f;
			 float MaxTransTotal=10f;
			 int NumP=0;
			 
			 
                         
			 while (values.hasNext()) {
				 String[] record = values.next().toString().split(",");
				 NumP+=Integer.parseInt(record[0]); 
				 TransTotal = Float.parseFloat(record[1]);
				 if(MinTransTotal>TransTotal){
					 MinTransTotal = TransTotal;
				 }
				 if(MaxTransTotal<TransTotal){
					 MaxTransTotal = TransTotal;
				 }
	 			 
			 }

            String output_value =NumP +","+ MinTransTotal+","+ MaxTransTotal ;
			 
			 output.collect(key, new Text(output_value));
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Path firstPath = new Path(args[0]);
		 Path finalPath = new Path(args[1]);
		 JobConf conf = new JobConf(query4_1.class);
		 conf.setJobName("query4_1");
		 
		 conf.setOutputKeyClass(IntWritable.class);
		 conf.setOutputValueClass(Text.class);
		 conf.setMapperClass(Map.class);
		 conf.setReducerClass(Reduce.class);
		

		 conf.setInputFormat(TextInputFormat.class);
		 conf.setOutputFormat(TextOutputFormat.class);
		 DistributedCache.addCacheFile(new URI("/proj1/customer/customer.txt"),conf);
		 FileInputFormat.setInputPaths(conf, firstPath);
		 FileOutputFormat.setOutputPath(conf,finalPath);
		 
		 JobClient.runJob(conf);

		 }
}

		



