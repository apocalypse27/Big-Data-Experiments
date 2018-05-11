package top10FriendsPackage;



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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.log4j.Logger;
import org.apache.commons.lang3.*;


public class findTop10Count {
	private static String FRIENDS_SPLIT=",";
	private static String CUR_FRIEND_SPLIT="\t";
	public static class Map_1 extends Mapper<Object,Text,Text,Text>
	{
		private static final Logger logger=Logger.getLogger(Map_1.class);
		private static ArrayList<String> outputKey=new ArrayList<>();
		private static Text friendsList=new Text();
		
		@Override
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException
		{
			MyComparator cmp=new MyComparator();
			String[] splitArray=values.toString().split(CUR_FRIEND_SPLIT);
			if(splitArray.length==2)
			{
				String curFriend=splitArray[0];
				friendsList.set(splitArray[1]);
				String[] otherFriendsArray=splitArray[1].split(FRIENDS_SPLIT);
				for(String friend:otherFriendsArray)
				{
					if(cmp.compare(friend,curFriend)<0)
					{
						outputKey.clear();
						outputKey.add(friend);
						outputKey.add(curFriend);
						logger.debug("Adding "+ friend +"  "+ curFriend);
						context.write(new Text(StringUtils.join(outputKey,",")), friendsList);
					}
					else
					{
						outputKey.clear();
						outputKey.add(curFriend);
						outputKey.add(friend);
						logger.debug("Adding "+ curFriend + friend);
						context.write(new Text(StringUtils.join(outputKey,",")), friendsList);
					}
				}	
			}
		}
	}
	
	public static class Reduce_1 extends Reducer<Text,Text,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();
		@Override
		public  void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException
		{
			
			String[] friendsList=new String[2];
			int i=0;
			int j=0;
			for(Text val:values)
			{
				friendsList[i++]=val.toString();
			}
			String[] friend_1_List=friendsList[0].split(FRIENDS_SPLIT);
			String[] friend_2_List=friendsList[1].split(FRIENDS_SPLIT);
			for(String friend:friend_1_List)
			{
				if(Arrays.asList(friend_2_List).contains(friend)){
					j++;
				}
			}
			result.set(j);
			context.write(key,result);
			
		}
	}
	
	
	public static class Map_2 extends Mapper<LongWritable,Text,NullWritable,Text>
	{
		
		int countValue;
		outputList mapperList=new outputList();
		@Override
		public void map(LongWritable key, Text values,Context context) throws IOException,InterruptedException
		{			
			if(values.toString().length()!=0)
			{
				String[] splitArray=values.toString().split(CUR_FRIEND_SPLIT);
				if(splitArray.length==2)
				{
					countValue=Integer.parseInt(splitArray[1]);
					mapperList.add(splitArray[0],countValue);
				}					
			}			
		}
		@Override
		public void cleanup(Context context) throws IOException,InterruptedException
		{
			for(keyValuePair pair:mapperList.opList)
			{
				context.write(NullWritable.get(),new Text(pair.toString()));
			}
			mapperList.clear();
		}
	}
	
	public static class Reduce_2 extends Reducer<NullWritable,Text,Text,Text>
	{
		outputList reducerList=new outputList();
		@Override
		public void reduce(NullWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException 
		{
			reducerList.clear();
			String[] splitValues;
			for(Text v:values){
				splitValues=v.toString().split(CUR_FRIEND_SPLIT);
				reducerList.add(splitValues[0], Integer.parseInt(splitValues[1]));
			}
			Collections.reverse(reducerList.opList);
			for(keyValuePair pair:reducerList.opList)
			{
				context.write(new Text(pair.key),new Text(pair.Countvalue + ""));
			}
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
	  	System.err.println("Usage: Top10 Friends <in> <out>");
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
	  	Job job1 = new Job(conf,"Step 1 : Finding mutual Friends");
	  	job1.setJarByClass(findTop10Count.class);
	  	job1.setMapperClass(Map_1.class);
	  	job1.setReducerClass(Reduce_1.class);
	  	job1.setOutputKeyClass(Text.class);
	  	job1.setOutputValueClass(Text.class);
	  	//set the HDFS path of the input data
	  	FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"/temp"));
	  	//Wait till job completion
	  	/*Provides access to configuration parameters*/
	  	job1.waitForCompletion(true);
	  	//System.exit(job1.waitForCompletion(true) ? 0 : 1);
	  	/*******/
	  	/*Start the next job*/
	  	/*********/
	  	Job job2 = new Job(conf, "Step 2: Frequency");
	  	job2.setJarByClass(findTop10Count.class);
	  	job2.setMapperClass(Map_2.class);
	  	job2.setReducerClass(Reduce_2.class);
	  	job2.setNumReduceTasks(1);
	  	//job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	  	job2.setOutputKeyClass(Text.class);
	  	job2.setOutputValueClass(Text.class);
	  	job2.setMapOutputKeyClass(NullWritable.class);
	  	job2.setMapOutputValueClass(Text.class);
	  	//job2.setInputFormatClass(SequenceFileInputFormat.class);
	  	FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/temp"));
	  	FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/out2"));
	  	if (!job2.waitForCompletion(true))
	  	{
	  	  System.exit(1);
	  	  }
	}


}
