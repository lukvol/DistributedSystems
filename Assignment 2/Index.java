package assignment2;

//Lukas Vollenweider
//13-751-888

import java.io.*;
import java.util.HashMap;
import java.util.Map;
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

public class Index extends Configured implements Tool {
	
	public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
		//Object: Input key type
		//Text: Input value type
		//Text: Output key type
		//Text: Output value type

		//we save the words and their occurrences per id into a HashMap
		//since it is perfect for that
		//it can't have multiple keys with the same value
		//it has a associated value 
		//and one can check a lot faster if it contains a key then with an array
		HashMap<String, Integer> wordCounterPerId;
		
		/**
		 * sets up the Mapper Instance
		 * @Param context: the almighty context
		 */
		@Override
		public void setup(Context context) throws IOException {
			wordCounterPerId = new HashMap<String, Integer>();
		}

		/**
		 * Processes the file plot_summaries.txt line by line
		 * @Param key: the input key type
		 * @Param value: the input value type
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//each line contains a movie id, followed by the summary --> see readme
			String line = value.toString();
			
			//replace those characters with a space character
			line = line.replaceAll(",", " ");
			line = line.replaceAll("\\.", " ");
			line = line.replaceAll("-", " ");
			
			StringTokenizer tokenizer = new StringTokenizer(line);

			//get the movieID
			int movieId = Integer.parseInt(tokenizer.nextToken());
			
			//Iterate through the summary
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken().toLowerCase();

				int occurrences = 0;
				//we check our HashMap if it contains the current word
				//if it does, we set occurrences to the counter
				if (wordCounterPerId.containsKey(word)) {
					occurrences = wordCounterPerId.get(word);
				}
				//we increase occurrences by 1
				//if we found the word in the HashMap it will be the same as counter + 1
				//if not it is 0 + 1 = 1
				wordCounterPerId.put(word, occurrences + 1);
			}
			
			//from: http://stackoverflow.com/a/1066607
			//we iterate through the map...
			for (Map.Entry<String, Integer> entry : wordCounterPerId.entrySet()) {
				//... and write word (getKey()) as the key and movieId,Occurrences as value into the context
				context.write(new Text(entry.getKey()), new Text(movieId + "," + entry.getValue()));
			}
			//we clear the HashMap to be sure that it is empty for the next map instance
			wordCounterPerId.clear();
		}
	}

	public static class CountReducer extends Reducer<Text, Text, Text, Text> {
		//Text: Input key type
		//Text: Input value type
		//Text: Output key type
		//Text: Output value type
		
		/**
		 * Processes the values for each key (word)
		 * @Param key: The output key from the mapper
		 * @Param value: The collection of values for a single key
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String reversedIndex = "";
			for (Text value : values) {
				//since our value is in format: movieID,occurrence, we
				//can append it directly to the resulting string and only need
				//to add a space character afterwards to get the desired formation of the output
				reversedIndex = reversedIndex + value + " ";
			}
			//we remove the last character, which is a space character
			reversedIndex = reversedIndex.substring(0, reversedIndex.length() - 1);
			//we write it to the output file
			context.write(key, new Text(reversedIndex));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
						
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		Path plotData = new Path(args[0]+"data/plot_summaries.txt");
		
		Path outputDestination = new Path(args[0] + args[1]);
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}
		
		/**
		 * Job 1
		 * Counts occurrences from words per plot summary
		 * and saves the result in the following format: word\tmovieID,occurrence movieID,occurrence ...
		 */
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(Index.class);
		
		if (args.length > 2){
			job1.setNumReduceTasks(Integer.parseInt(args[2]));
		} else {
			job1.setNumReduceTasks(1);
		}
		
		job1.setMapperClass(MovieMapper.class);
		job1.setReducerClass(CountReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, plotData);
		FileOutputFormat.setOutputPath(job1, outputDestination);
						
		int status = job1.waitForCompletion(true) ? 0 : 1;
		
		return status;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Index(), args);
	}
}