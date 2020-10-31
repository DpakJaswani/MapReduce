import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriendsTop {
	public static class TopMap extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text  opKey = new Text();
		private Text opValue = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] myData = value.toString().split("\t");
			int usr = Integer.parseInt(myData[0]);
			
			if(myData.length == 2)
			{
				String friendsValue = myData[1];
				String[] friendsList = myData[1].toString().split(",");
				
				for(String data: friendsList)
				{
					int friend = Integer.parseInt(data);
					if(usr < friend)
					{
						opKey.set(usr+","+friend);
					}
					else
					{
						opKey.set(friend+","+usr);
					}
					
					opValue.set(friendsValue);
					context.write(opKey, opValue);
				}
			}
		}
	}
	
	public static class TopReduce extends Reducer<Text, Text, Text, IntWritable>
	{
		private Text opKey = new Text();
		private IntWritable opValue = new IntWritable();
		private int count = 0;
		private HashMap<String, Integer> countMap = new HashMap<String, Integer>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Iterator<Text> friendUserId = values.iterator();
			Text[] friendsList = new Text[2];
			ArrayList<String> mutualFriends = new ArrayList<String>();
			int index = 0;
			Iterator<Text> iterator = values.iterator();
			
			while(friendUserId.hasNext())
			{
				friendsList[index] = new Text();
				friendsList[index].set(iterator.next());
				index++;
			}
			
			String[] friendUserId1 = friendsList[0].toString().split(",");
			String[] friendUserId2 = friendsList[1].toString().split(",");
			
			for(int i=0; i< friendUserId1.length; i++)
			{
				for(int j=0; j< friendUserId2.length; j++)
				{
					if(friendUserId1[i].equals(friendUserId2[j]))
					{
						mutualFriends.add(friendUserId1[i]);
					}
				}
			}
			
			countMap.put(key.toString(), mutualFriends.size());
			
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException
		{
			int max = countMap.values().stream().max(Comparator.naturalOrder()).get();
			HashMap<String, Integer> sortedMap = countMap.entrySet().stream().filter(e-> e.getValue() == max).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,(e1,e2)->e1,LinkedHashMap::new));
			sortedMap.entrySet().stream().forEach(action -> {
				opKey.set(action.getKey());
				opValue.set(action.getValue());
				
				try
				{
					context.write(opKey, opValue);
					
				} 
				catch(IOException e)
				{
					e.printStackTrace();
				}
				catch(InterruptedException e)
				{
					e.printStackTrace();
				}
				count++;
			});
			super.cleanup(context);
		}
		
	}
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		
		if(otherArgs.length != 2)
		{
			System.err.println("Usage: Mutual friends input/output");
			System.exit(2);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "Top Mutual Friends");
		job.setJarByClass(MutualFriendsTop.class);
		job.setMapperClass(TopMap.class);
		job.setReducerClass(TopReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	

}
