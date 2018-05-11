package minMaxMedian;

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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class findMinMaxMedian {
	public static class Map extends Mapper<LongWritable,Text,myNumberKey,IntWritable>
	{
		IntWritable newSortingKey=new IntWritable();
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException
		{
			newSortingKey.set(Integer.parseInt(value.toString()));
			myNumberKey newKey=new myNumberKey(newSortingKey);
			context.write(newKey,newSortingKey);
		}
	}
	
	public class myNumberKeyPartitioner extends Partitioner<myNumberKey, IntWritable>{
	    @Override
	    public int getPartition(myNumberKey myKey, IntWritable value, int numPartitions) {
	        return myKey.naturalKey.hashCode() % numPartitions;
	    }
	}
	
	public static class Reduce extends Reducer<myNumberKey,IntWritable,Text,Text>
	{
		ArrayList<Integer> reducerOpList=new ArrayList<>();
		@Override
		public void reduce(myNumberKey key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int count=0;
			Double median;
			Integer max;
			Integer min;
			for(IntWritable v:values)
			{
				reducerOpList.add(v.get());
				count++;
			}
			
			if(count%2==0)
			{
				median=(double)(reducerOpList.get(count/2)+reducerOpList.get((count/2)-1))/2;
			}
			else
			{
				median=(double)reducerOpList.get(count/2);
			}
			max=reducerOpList.get(reducerOpList.size()-1);
			min=reducerOpList.get(0);
			
			context.write(new Text("Max: "),new Text(max.toString()));
			context.write(new Text("Min: "),new Text(min.toString()));
			context.write(new Text("Median: "),new Text(median.toString()));
			
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
	  	System.err.println("Usage: MinMaxMedian <in> <out>");
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
	  	Job job = new Job(conf,"Max Min and Median");
	  	job.setJarByClass(findMinMaxMedian.class);
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
	  	// set output key type
	  	job.setOutputKeyClass(Text.class);
	  	// set output value type
	  	job.setOutputValueClass(Text.class);
	  	job.setMapOutputKeyClass(myNumberKey.class);
	  	job.setMapOutputValueClass(IntWritable.class);
	  	job.setPartitionerClass(myNumberKeyPartitioner.class);
	  	job.setGroupingComparatorClass(myKeyGroupComparator.class);
	  	//job.setSortComparatorClass(CompositeKeyComparator.class)
	  	//set the HDFS path of the input data
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	//Wait till job completion
	  	/*Provides access to configuration parameters*/
	  	
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
	
	

}
