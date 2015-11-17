package assignment2;

//Lukas Vollenweider
//13-751-888

import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordFreq extends Configured implements Tool {

	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
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
			line = line.replaceAll(",", " ");
			line = line.replaceAll("\\.", " ");
			line = line.replaceAll("-", " ");
			
			//read a new line
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			//we don't exclude the movieID, since it is not written, that we should
			while (tokenizer.hasMoreTokens()) {
				//safe the token 
				String keyForReduce = tokenizer.nextToken().toLowerCase();
				//we give every word the value 1
				//which is unnecessary but we do it anyway
				IntWritable valueForReduce = new IntWritable(1);
				
				//pass the word with its value to the reducer
				context.write(new Text(keyForReduce), valueForReduce);
			}
		}
	}

	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		//Text: Input key type
		//IntWritable: Input value type
		//Text: Output key type
		//IntWritable: Output value type
		
		/**
		 * Processes the values for each key (word) counts the frequency of it and writes it into the context
		 * Called by the first job
		 * @Param key: The output key from the mapper
		 * @Param value: The collection of values for a single key
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//we loop through every value for a given key and add its value (always 1) to the sum
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			//write the result (word\tfrequency) to a output file
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class ResultMapper extends Mapper<Object, Text, IntWritable, Text> {
		/**
		 * Processes the values for each key word and write it into the context
		 * Called by the second job
		 * @Param key: The input key type
		 * @Param value: the input value type
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//This one does hardly anything, sadly it need to do something
			//so we do need it
			//it reads the lines from the temp file which CountReducer created
		
			String line = value.toString();
			
			//each line is in the following format: word\tfrequency
			//so there are no dynamic lines and therefore it is nicer
			//to split it and save the result in an array
			String[] wordFrequencies = line.split("\t");
			
			String word = wordFrequencies[0];
			int occurrences = Integer.parseInt(wordFrequencies[1]);
			
			//Map/Reduce does sorting by key but we need sort by value
			//instead of writing some bulky algorithms to sort by value
			//we just swap the key and the value
			context.write(new IntWritable(occurrences), new Text(word));
		}
	}
	
	public static class ResultReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		//IntWritable: Input key type
		//Text: Input value type
		//Text: Output key type
		//IntWritable: Output value type

		/**
		 * Processes the values for each key (word) and writes it into the context
		 * Called by the second job
		 * @Param key: The output key from the mapper
		 * @Param value: The collection of values for a single key
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//we get the sorted values (descending order) from the mapper
			//therefore we only have to write it to the outputfile
			for (Text value: values) {
				//we have to swap the keys and values again to write it in the format word\tfrequency
				context.write(value, key);
			}
		}
	}
	//idea from: http://stackoverflow.com/a/18156438
	public static class ReverseSorting extends WritableComparator{
		protected ReverseSorting() {
			super(IntWritable.class, true);
		}
			
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable key1, WritableComparable key2) {
			IntWritable occ1 = (IntWritable) key1;
			IntWritable occ2 = (IntWritable) key2;
				
			//I don't know what the resulting value is (didn't find it in the api, probably 1 for greater
			//0 for equal and -1 for smaller)
			//but I understand the system:
			//It is by default sorted in ascending order				
			//so if we just negate the value, we get the the reverse which is descending order which is
			//what we want
			return (-1) * occ1.compareTo(occ2);
		}	
	}
	
	@Override
	public int run(String[] args) throws Exception {
		final String tempLocation = args[0]+"temp";
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		
		Path tempDestination = new Path(tempLocation);
		if (fs.exists(tempDestination)) {
			fs.delete(tempDestination, true);
		}
		
		Path outputDestination = new Path(args[0] + args[1]);
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}
		/**
		 * Job 1
		 * Counts occurrences from words
		 * and saves the result in a temp file
		 */
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(WordFreq.class);
		
		if (args.length > 2){
			job1.setNumReduceTasks(Integer.parseInt(args[2]));
		} else {
			job1.setNumReduceTasks(1);
		}
		
		job1.setMapperClass(CountMapper.class);
		job1.setReducerClass(CountReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]+"data/plot_summaries.txt"));
		FileOutputFormat.setOutputPath(job1, tempDestination);
		
		job1.waitForCompletion(true);

		/**
		 * Job 2
		 * Sorts the words by frequency in descending order
		 */
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(WordFreq.class);
		
		if (args.length > 2){
			job2.setNumReduceTasks(Integer.parseInt(args[2]));
		} else {
			job2.setNumReduceTasks(1);
		}
		
		job2.setMapperClass(ResultMapper.class);
		job2.setReducerClass(ResultReducer.class);
		
		job2.setSortComparatorClass(ReverseSorting.class);
		
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job2, tempDestination);
		FileOutputFormat.setOutputPath(job2, outputDestination);
		
		int status = job2.waitForCompletion(true) ? 0 : 1;
		
		//we delete our temp file and directory
		if (fs.exists(tempDestination)) {
			fs.delete(tempDestination, true);		
		} 
		
		return status;

	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordFreq(), args);
	}
}