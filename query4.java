package mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import java.lang.*;

public class query4{
	public static class CustomerMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			String line = value.toString();
			String[] record = line.split(",");
			
			
			int CustID = Integer.parseInt(record[0]);
			IntWritable output_key = new IntWritable(CustID);
                        Text output_value = new Text("A,"+record[3]+",1");
                        output.collect(new IntWritable(CustID),output_value);
			
		}
	}
	
	public static class TransactionsMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			String line = value.toString();
			String[] record = line.split(",");
			int CustID = Integer.parseInt(record[1]);
			IntWritable output_key = new IntWritable(CustID);
                        Text output_value = new Text("B,"+record[2]);
                        output.collect(new IntWritable(CustID),output_value);
		}
	}


	
	public static class Reduce1 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			 float TransTotal = 0.0f;
			 float MinTransTotal=1000f;
			 float MaxTransTotal=10f;
			 int NumP=0;
			 int CountryCode =0;
			 
                         
			 while (values.hasNext()) {
				 String line = values.next().toString();
				 String[] record = line.split(",");
				 if(record[0].equals("A")){
					 CountryCode = Integer.parseInt(record[1]);
					 NumP=Integer.parseInt(record[2]); 
				 }else if(record[0].equals("B")){
					 TransTotal = Float.parseFloat(record[1]);
					 if(MinTransTotal>TransTotal){
						 MinTransTotal = TransTotal;
					 }
					 if(MaxTransTotal<TransTotal){
						 MaxTransTotal = TransTotal;
					 }
				 }
	 			 
			 }

            String output_value = CountryCode +","+NumP +","+ MinTransTotal+","+ MaxTransTotal ;
			 
			 output.collect(key, new Text(output_value));
		}
	}
	
public static class SecondMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			String line = value.toString();
			String[] record = line.split("\t");
			String record_value = record[1];
                        String[] record_value_items = record_value.split(",");
 			int CountryCode = Integer.parseInt(record_value_items[0]);
			IntWritable output_key = new IntWritable(CountryCode);
            Text output_value = new Text(record_value_items[0]+","+record_value_items[1]+","+record_value_items[2]);
            output.collect(new IntWritable(CountryCode),output_value);
		}
	}
	public static class Reduce2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			 int MumPerson =0;
			 Float MinTransTotal=1000f;
			 Float MaxTransTotal=10f;
			 float TransTotal1= 0f;
			 float TransTotal2= 0f;
                         
			 while (values.hasNext()) {
				 String line = values.next().toString();
				 String[] record = line.split(",");
				 TransTotal1= Float.parseFloat(record[1]);
				 TransTotal2= Float.parseFloat(record[2]);
	
				 MumPerson +=Integer.parseInt(record[0]);
				 if(MinTransTotal>TransTotal1){
					 MinTransTotal = TransTotal1;
				 }
				 if(MaxTransTotal<TransTotal2){
					 MaxTransTotal = TransTotal2;
				 }
			 }

            String output_value = MumPerson +","+ MinTransTotal+ ","+ MaxTransTotal;
			 
			 output.collect(key,new Text(output_value));
		}
	}

	
	public static void main(String[] args) throws Exception {
		 Path firstPath = new Path(args[0]);
		 Path secondPath = new Path(args[1]);
		 Path outputPath = new Path(args[2]);
		 Path intermidatePath = new Path(args[3]);
		 JobConf conf = new JobConf(query4.class);
		 conf.setJobName("query4");
		 
		 conf.setOutputKeyClass(IntWritable.class);
		 conf.setOutputValueClass(Text.class);
		 
		 conf.setMapperClass(CustomerMap.class);
		 conf.setMapperClass(TransactionsMap.class);
		 conf.setReducerClass(Reduce1.class);
		 
		 conf.setInputFormat(TextInputFormat.class);
		 conf.setOutputFormat(TextOutputFormat.class);
		 MultipleInputs.addInputPath(conf,firstPath,TextInputFormat.class,CustomerMap.class);
		 MultipleInputs.addInputPath(conf,secondPath,TextInputFormat.class,TransactionsMap.class);
		 FileOutputFormat.setOutputPath(conf,intermidatePath);
		 
		 JobClient.runJob(conf);
		 
		 JobConf conf2 = new JobConf(query4.class);
		 conf2.setJobName("query4_2");
		 
		 conf2.setOutputKeyClass(IntWritable.class);
		 conf2.setOutputValueClass(Text.class);
		 
		 conf2.setMapperClass(SecondMap.class);
		 conf2.setReducerClass(Reduce2.class);

		 conf2.setInputFormat(TextInputFormat.class);
		 conf2.setOutputFormat(TextOutputFormat.class);
		 FileInputFormat.setInputPaths(conf2, intermidatePath);
		 FileOutputFormat.setOutputPath(conf2, outputPath);
		 
		 JobClient.runJob(conf2);


		
	}
}