import java.io.IOException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.GenericOptionsParser;



import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class RsJoin {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text  opKey = new Text();
		private Text opValue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] keyValue = line.split("\t");
			if(keyValue.length >= 2)
			{
				String user = keyValue[0];
				String[] friends = keyValue[1].toString().split(",");
				for(String friend : friends )
				{
					context.write(new Text(user), new Text(friend));

				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		HashMap<String, Integer> ageMap = new HashMap<String, Integer>();
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			
			try {

			//Path part=new Path(conf.get("userData"));//Location of file in HDFS
			URI[] paths = context.getCacheFiles();
			
			System.out.println("URI:" + paths);
			Path part=new Path(paths[0].getPath());
			FileSystem fs = FileSystem.get(new Configuration());    
		    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(part)));
		    String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] userinfo = line.split(",");
		        	if(userinfo.length == 10)
		        	{
		        		String dob = userinfo[9];
		        		LocalDate today = LocalDate.now();
		        		LocalDate userDay = LocalDate.of(Integer.parseInt(dob.split("/")[2]), Integer.parseInt(dob.split("/")[0]), Integer.parseInt(dob.split("/")[1]));
		        		int age = Period.between(userDay, today).getYears();
		        		ageMap.put(userinfo[0],age);
		        	}
		        	
		        	line=br.readLine();
		        }
			}catch(Exception e)
			{
				System.err.println(e);
			}
		    }
			
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			ArrayList<Integer> ageList = new ArrayList<Integer>();

			for(Text friends: values)
			{

				String[] friendsArray = friends.toString().split(",");
				for(String friendId : friendsArray)
				{
					ageList.add(ageMap.get(friendId));
				}
			}
			String maxAge = Collections.max(ageList).toString();
			context.write(key, new Text(maxAge));

		}
				
	}
	public static void main(String[] args) throws Exception
	{
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		
		if(otherArgs.length != 3)
		{
			System.err.println("Usage: input userdata output");
			System.exit(2);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "Top Mutual Friends");
		job.setJarByClass(RsJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.addCacheFile(new Path(otherArgs[1]).toUri());
		
		configuration.set("userData",otherArgs[1]);
		
		System.out.println(configuration.get("userData"));

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
