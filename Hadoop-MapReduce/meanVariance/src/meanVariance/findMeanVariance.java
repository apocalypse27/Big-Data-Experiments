package meanVariance;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class findMeanVariance {
	private static final String SPLITTER="\t";
	public static class Map extends Mapper<LongWritable,Text,NullWritable,Text>
	{
		@Override
		public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException
		{
			Double mult=Math.pow(Long.parseLong(value.toString()),2);
			context.write(NullWritable.get(),new Text("1"+SPLITTER+value.toString()+SPLITTER+mult.toString()));
		}
	}
	
	public static class Combine extends Reducer<NullWritable,Text,NullWritable,Text>
	{
		Long sum;
		Double squareSum;
		Long count;
		@Override
		public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			String[] splitValues=new String[3];
			sum=0L;
			squareSum=0.0;
			count=0L;
			for(Text v:values)
			{
				splitValues=v.toString().split(SPLITTER);
				if(splitValues.length==3)
				{
					count+=Integer.parseInt(splitValues[0]);
					sum+=Integer.parseInt(splitValues[1]);
					squareSum+=Double.parseDouble(splitValues[2]);
				}
				
			}
			
			context.write(NullWritable.get(),new Text(count+SPLITTER+sum+SPLITTER+squareSum));
		}
	}
	
	public static class Reduce extends Reducer<NullWritable,Text,Text,Text>
	{
		Long sum;
		Double squareSum;
		Long count;
		@Override
		public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			String[] splitValues=new String[3];
			sum=0L;
			squareSum=0.0;
			count=0L;
			for(Text v:values)
			{
				splitValues=v.toString().split(SPLITTER);
				if(splitValues.length==3)
				{
					count+=Integer.parseInt(splitValues[0]);
					sum+=Long.parseLong(splitValues[1]);
					squareSum+=Double.parseDouble(splitValues[2]);
				}
				
			}
			
			Long mean=sum/count;
			Double meanSquare=squareSum/count;
			Double variance=meanSquare-Math.pow(mean,2);
			context.write(new Text(mean+""),new Text(""+variance));
			
		}
	}
	
	// Driver program
	  	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
	  	System.err.println("Usage: findMeanVariance <in> <out>");
	  	System.exit(2);
	  	}
	  	
	  	/*Creating Filesystem object with the configuration*/
	  	FileSystem fs = FileSystem.get(conf);
	  	/*Check if output path (args[1])exist or not*/
	  	if(fs.exists(new Path(args[1]))){
	  	   /*If exist delete the output path*/
	  	   fs.delete(new Path(args[1]),true);
	  	}
	  	// create a job with name "wordcount"
	  	Job job = new Job(conf, "MeanVariance");
	  	job.setJarByClass(findMeanVariance.class);
	  	job.setMapperClass(Map.class);
	  	job.setCombinerClass(Combine.class);
	  	job.setReducerClass(Reduce.class);
	  	job.setMapOutputKeyClass(NullWritable.class);
	  	job.setMapOutputValueClass(Text.class);
	  	// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
	  	// set output key type
	  	job.setOutputKeyClass(Text.class);
	  	// set output value type
	  	job.setOutputValueClass(Text.class);
	  	//set the HDFS path of the input data
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	//Wait till job completion
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
	  	}
	
	
}



