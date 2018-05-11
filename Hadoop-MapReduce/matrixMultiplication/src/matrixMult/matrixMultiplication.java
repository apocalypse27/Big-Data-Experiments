package matrixMult;


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
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

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


public class matrixMultiplication {
	static matrixType A=new matrixType();
	static matrixType B=new matrixType();
	
	public static class Map_1 extends Mapper<LongWritable,Text,IntWritable,Text>
	{
        String[] splitValues=new String[4];
		@Override
		public void map(LongWritable Key,Text value,Context context) throws IOException,InterruptedException
        {
			//System.out.println("Preeti says reached here map");
			splitValues=value.toString().split(",");
        	IntWritable outputKey = new IntWritable();
        	Text outputValue = new Text();
        	for(int i=0;i<splitValues.length;i++)
        	{
        		splitValues[i]=splitValues[i].trim();
        	}
        	if(splitValues[0].equals("A"))
        	{
        		outputKey.set(Integer.parseInt(splitValues[2]));
        		outputValue.set("A"+","+splitValues[1]+","+splitValues[3]);
        		//System.out.println("OutputKey "+outputKey.toString() + " OUtputValue "+outputValue.toString());
                
        		context.write(outputKey, outputValue);
        	}
        	else
        	{
        		outputKey.set(Integer.parseInt(splitValues[1]));
        		outputValue.set("B" +","+splitValues[2]+","+splitValues[3]);
        		//System.out.println("OutputKey "+outputKey.toString() + " OUtputValue "+outputValue.toString());
                
        		context.write(outputKey,outputValue);
        	}	
        	
        }
	}
	
	public static class Reduce extends Reducer<IntWritable,Text,Text,IntWritable>
	{		
		@Override
		public  void reduce(IntWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException
		{
			//System.out.println("Preeti says reached here reduce");
			String[] splitValues;
			ArrayList<Entry<Integer,Integer>> A_List=new ArrayList<>();
			ArrayList<Entry<Integer,Integer>> B_List=new ArrayList<>();
			for (Text val : values) {
				splitValues = val.toString().split(",");
                if (splitValues[0].equals("A")) {
                	A_List.add(new SimpleEntry<Integer, Integer>(Integer.parseInt(splitValues[1]), Integer.parseInt(splitValues[2])));
                	//System.out.println("Adding to A list key: "+ Integer.parseInt(splitValues[1]) + "val: " + Integer.parseInt(splitValues[2]));
                } else {
                	
                	B_List.add(new SimpleEntry<Integer, Integer>(Integer.parseInt(splitValues[1]), Integer.parseInt(splitValues[2])));
                	//System.out.println("Adding to B list key: "+ Integer.parseInt(splitValues[1]) + "val: " + Integer.parseInt(splitValues[2]));
                    
                }
                }
			
			String i;
            int a_ij;
            String k;
            int b_jk;
            Text outputKey = new Text();
            IntWritable outputValue = new IntWritable();
            for (Entry<Integer, Integer> a : A_List) 
            {
                i = Integer.toString(a.getKey());
               // System.out.println("i is "+i);                
                a_ij = a.getValue();
                //System.out.println("aij is "+a_ij);
                for (Entry<Integer, Integer> b : B_List) 
                {
                    k = Integer.toString(b.getKey());
                   // System.out.println("k "+k);
                    b_jk = b.getValue();
                   // System.out.println("bjk is "+b_jk);
                    outputKey.set(i+","+k);
                    outputValue.set(a_ij*b_jk);
                    //System.out.println("OutputKey "+outputKey.toString() + " OUtputValue "+outputValue.toString());
                    context.write(outputKey, outputValue);
                }
             }
			
		}
	}
	
	public static class Map_2 extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			Text outputKey=new Text();
			IntWritable outputValue= new IntWritable();
			String[] splitValues=value.toString().split("\t");
			outputKey.set(splitValues[0]);
			outputValue.set(Integer.parseInt(splitValues[1]));
			context.write(outputKey,outputValue);			
		}
	}
	
	public static class Reduce_2 extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		IntWritable outputValue=new IntWritable();
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{			
			int sum=0;
			for(IntWritable v:values)
			{
				sum+=v.get();
			}
			outputValue.set(sum);
			context.write(key,outputValue);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	// get all args
	  	
	  	if (otherArgs.length != 2) {
	  	System.err.println("Usage: Matrixmultiplication <in> <out>");
	  	System.exit(2);
	  	}
	  	/*Creating Filesystem object with the configuration*/
	  	FileSystem fs = FileSystem.get(conf);
	  	/*Check if output path (args[1])exist or not*/
	  	if(fs.exists(new Path(args[1]))){
	  	   /*If exist delete the output path*/
	  	   fs.delete(new Path(args[1]),true);
	  	}
	  	if(fs.exists(new Path(args[1]+"/temp"))){
		  	   /*If exist delete the output path*/
		  	   fs.delete(new Path(args[1]+"/temp"),true);
		  	}
	  	if(fs.exists(new Path(args[1]+"/out2"))){
		  	   /*If exist delete the output path*/
		  	   fs.delete(new Path(args[1]+"/out2"),true);
		  	}
	  	
	 // create a job with name "wordcount"
	  	Job job1 = new Job(conf,"Step 1: Multiplying");
	  	job1.setJarByClass(matrixMultiplication.class);
	  	job1.setMapperClass(Map_1.class);
	  	job1.setReducerClass(Reduce.class);
	  	job1.setOutputKeyClass(Text.class);
	  	// set output value type
	  	job1.setOutputValueClass(IntWritable.class);
	  	job1.setMapOutputKeyClass(IntWritable.class);
	  	job1.setMapOutputValueClass(Text.class);
	  	//set the HDFS path of the input data
	  	FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"/temp"));
	  	//Wait till job completion	
	  	job1.waitForCompletion(true);
	  	
	  	Job job2 = new Job(conf, "Step 2: Adding");
	  	job2.setJarByClass(matrixMultiplication.class);
	  	job2.setMapperClass(Map_2.class);
	  	job2.setReducerClass(Reduce_2.class);
	  	job2.setNumReduceTasks(1);
	  	//job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	  	job2.setOutputKeyClass(Text.class);
	  	job2.setOutputValueClass(IntWritable.class);
	  	job2.setMapOutputKeyClass(Text.class);
	  	job2.setMapOutputValueClass(IntWritable.class);
	  	//job2.setInputFormatClass(SequenceFileInputFormat.class);
	  	FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/temp"));
	  	FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/out2"));
	  	if (!job2.waitForCompletion(true))
	  	{
	  	  System.exit(1);
	  	  }
	}
	

}
