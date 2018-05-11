
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
import org.apache.log4j.Logger;
import org.apache.commons.lang3.*;


public class ques1 {
	private static String FRIENDS_SPLIT=",";
	private static String CUR_FRIEND_SPLIT="\t";
	
	public static class Map extends Mapper<Object,Text,Text,Text>
	{
		private static final Logger logger=Logger.getLogger(Map.class);
		private static ArrayList<String> outputKey=new ArrayList<>();
		private static Text friendsList=new Text();
		
		@Override
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException
		{
			
			MyComparator cmp=new MyComparator();
			String[] splitArray=value.toString().split(CUR_FRIEND_SPLIT);
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
	
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>
	{
		
		@Override
		public  void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException
		{
			
			String[] friendsList=new String[2];
			int i=0;
			for(Text val:values)
			{
				friendsList[i++]=val.toString();
			}
			String[] friend_1_List=friendsList[0].split(FRIENDS_SPLIT);
			String[] friend_2_List=friendsList[1].split(FRIENDS_SPLIT);
			
			ArrayList<String> mutualFriendsList=new ArrayList<>();
			for(String friend:friend_1_List)
			{
				if(Arrays.asList(friend_2_List).contains(friend)){
					mutualFriendsList.add(friend);
				}
			}
			context.write(key,new Text(StringUtils.join(mutualFriendsList,",")));
			
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
	  	System.err.println("Usage: MutuaFriends <in> <out>");
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
	  	Job job = new Job(conf,"mutualFriends");
	  	job.setJarByClass(ques1.class);
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
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
	  	/*Provides access to configuration parameters*/
	  	
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
