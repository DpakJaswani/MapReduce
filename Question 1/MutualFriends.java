import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriends {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>
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
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		private Text opValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Iterator<Text> friendUserId = values.iterator();
			ArrayList<String> givenPairs = new ArrayList<String>();
			int i = 0, count = 0;
			String mutualFriends = "";
			Text[] friendsArray = new Text[2];
			
			givenPairs.add("0,1");
			givenPairs.add("20,28193");
			givenPairs.add("1,29826");
			givenPairs.add("6222,19272");
			givenPairs.add("28041,28056");
			
			Iterator<Text> itr = values.iterator();
			
			while(friendUserId.hasNext())
			{
				friendsArray[i] = new Text();
				friendsArray[i].set(itr.next());
				i++;
			}
			
			String[] friendsUserId1 = friendsArray[0].toString().split(",");
			String[] friendsUserId2 = friendsArray[1].toString().split(",");
			
			for(int j=0; j<friendsUserId1.length; j++)
			{
				for(int k=0; k<friendsUserId2.length; k++)
				{
					if(friendsUserId1[j].equals(friendsUserId2[k]))
					{
						mutualFriends += friendsUserId1[j]+",";
						++count;
					}
				}
			}
			
			if(count == 0)
				opValue.set("");
			else
			{
				mutualFriends = mutualFriends.substring(0,mutualFriends.length()-1);
				opValue.set(mutualFriends);
				
			}
				
			if (key.toString().equals("0,1") ||
					key.toString().equals("20,28193") ||
					key.toString().equals("1,29826") ||
					key.toString().equals("6222,19272") ||
					key.toString().equals("28041,28056") )
			{
				context.write(key, opValue);
			}	
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration configuration = new Configuration();		
		String[] otherArguments = new GenericOptionsParser(configuration, args).getRemainingArgs();
		
		if(otherArguments.length !=2)
		{
			System.err.println("Use: Mutual Friends <ip> <op>");
			System.exit(0);
		}
		
		Job job = new Job(configuration, "Mutual Friends");
		
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArguments[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
	}
	
	

}
