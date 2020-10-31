import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InMemoryJoin {

// count word frequency and find the word id	
	
	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String,String> friendsmap = new HashMap<String,String>();
		HashMap<String,List<String>> usermap = new HashMap<String,List<String>>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().trim().split(","); //gets the two user ids from input file
			System.out.println("myData"+ mydata[0]);
			ArrayList<String> friendLists = new ArrayList<>();
			String user1Friends[] = friendsmap.get(mydata[0]).split(","); // get the friends of user 1
			String user2Friends[] = friendsmap.get(mydata[1]).split(","); // get the friends of user 2
			int length1 = user1Friends.length;
			int length2 = user2Friends.length;
			for(int j=0;j<length1;j++) {
				for(int k=0;k<length2;k++) {
					if(user1Friends[j].equals(user2Friends[k])) {
						friendLists.add(user1Friends[j]); // add the mutual friends to the list
					}
				}
			}
			String detailsList = "";
			for(String userId : friendLists) {
			      detailsList = detailsList+usermap.get(userId).toString();
			}
			outputKey = value;
			outputValue.set(detailsList.replace("][", ",").toString());
			
			context.write(outputKey, outputValue);
		}
		
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the mapper.
			Configuration conf = context.getConfiguration();
		
			Path part=new Path(conf.get("FRIENDSFILEPATH"));//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split("\t");
		        	if(arr.length<2)
		        		friendsmap.put(arr[0],"");
		        	else
		        		//put user id and friends
		        		friendsmap.put(arr[0], arr[1]);
		        	line=br.readLine();
		        }
		    }
		    
		    Path part1=new Path(conf.get("USERDATAFILEPATH"));//Location of file in HDFS
			FileStatus[] fss1 = fs.listStatus(part1);
		    for (FileStatus status : fss1) {
		    Path pt=status.getPath();
		    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		    String line;
		    line=br.readLine();
		    
		    while(line !=null) {
		    	String[] arr=line.split(",");
		    	List<String> userList=new ArrayList<String>();
		    	userList.add(arr[1]+":"+ arr[9]);
	        	usermap.put(arr[0], userList);
	        	line=br.readLine();
		    	}
		    }
		    
		    
		    
		}
	}
	
	
	

	public static class Reduce
	extends Reducer<Text,Text,Text,Text> {


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				System.out.println(key + " "+val);
				context.write(key, val);
			}
			 // emit <(keyword, wordID), word frequency>
		}
	}


	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: Inmemory join <in> <Friend's list> <User Data list> <out>");
			System.exit(2);
		}
		
		// the wordid.txt input path
		conf.set("FRIENDSFILEPATH",otherArgs[1]);
		conf.set("USERDATAFILEPATH",otherArgs[2]);
		
		// create a job with name "wordcount"
		Job job = new Job(conf, "inmemory");
		job.setJarByClass(InMemoryJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);


		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data (word.txt)
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path of the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

