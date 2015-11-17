package assignment2;

//Lukas Vollenweider
//13-751-888

import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordCount {

	public static class CountMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		//Object: Input key type
		//Text: Input value type
		//Text: Output key type
		//IntWritable: Output value type

		/**
		 * Processes the file plot_summaries.txt line by line. Called by the first job
		 * @Param key: the input key type
		 * @Param value: the input value type
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//ignore following special characters
			String line = value.toString();
			
			//read a new line
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			//exclude the movieID (it's always the first item)
			tokenizer.nextToken();
			
			//we iterate through the words from the summary
			while (tokenizer.hasMoreTokens()) {
				//we are interested in the total number of words
				//so we don't care about the words themselves
				//therefore there is no reason to save the result
				tokenizer.nextToken();
				
				//we pass as key a "1" and as value a 1
				context.write(new Text("1"), new IntWritable(1));
			}
		}
	}

	public static class CountReducer extends
			Reducer<Text, IntWritable, IntWritable, NullWritable> {
		
		/**
		 * Processes the values for each key (1) and increases sum 
		 * @Param key: The output key from the mapper
		 * @Param value: The collection of values for a single key
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//since the reducer gets all the values for a specific key
			//and we set the key 1 for every word
			//values contains every word of the file
			//so we only have to iterate through values and increase sum
			//for each step
			int sum = 0;
			for (@SuppressWarnings("unused") IntWritable val : values) {
				sum++;
			}
			
			//we are only interested in the total number of words, therefore
			//we pass NullWritable as value, which writes nothing
			context.write(new IntWritable(sum), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		//Configure the job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		FileSystem fs = FileSystem.get(conf);

		job.setJarByClass(WordCount.class);
		
		//Set number of reducer
		job.setNumReduceTasks(1);
		if (args.length > 2){
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}
		
		//Configures the mapper
		job.setMapperClass(CountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//Configures the reducer
		job.setReducerClass(CountReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// handle (e.g. delete) existing output path
		Path outputDestination = new Path(args[0]+args[1]);
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}

		// set input and output path & start job1
		FileInputFormat.addInputPath(job, new Path(args[0]+"data/plot_summaries.txt"));
		FileOutputFormat.setOutputPath(job, outputDestination);
		job.waitForCompletion(true);
	}
}