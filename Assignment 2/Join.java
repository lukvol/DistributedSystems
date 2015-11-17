package assignment2;

//Lukas Vollenweider
//13-751-888

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Join extends Configured implements Tool {

	public static class MovieMapper extends Mapper<Object, Text, IntWritable, Text> {
		//Object: Input key type
		//Text: Input value type
		//IntWritable: Output key type
		//Text: Output value type

		/**
		 * Processes the file movie.metadata.tsv line by line
		 * @Param key: the input key type
		 * @Param value: the input value type
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			//each line contains exactly 9 elements, separated by tabs
			//if there is an element missing, the tab is still there
			//i.e.: wikiID \t\t movie name
			//so we are save to split the line by tabs and access the desired element
			//by its index number. 
			//If there is an element missing, the element at that index is null
			String[] movie = line.split("\t");			
			
			//wikiID is always the first element
			int movieId = Integer.parseInt(movie[0]);
			
			//there is a problem with the multipleinputs-procedure
			//one does not know, which mapper (MovieMapper or ActorMapper) gets executed first
			//and we have no way to distinguish the movie from the actors name
			//so to get the output in the desired format, we need to mark the movie
			//we do this by appending the movieID to the MovieFlag string to get a relatively unique ID
			//we should be save to use that as a distinction since there are no people with a wikipedia
			//id as name
			String movieFlag = "MovieFlag" + "_" + movie[0];
			//movie name is always the third element
			String movieName = movie[2] + movieFlag;
			
			//write the movieID as well as the MovieName (flagged) to the context
			//which will then be passed to the reducer
			context.write(new IntWritable(movieId), new Text(movieName));
		}
	}
	
	public static class ActorMapper extends Mapper<Object, Text, IntWritable, Text> {
		//Object: Input key type
		//Text: Input value type
		//IntWritable: Output key type
		//Text: Output value type

		/**
		 * Processes the file character.metadata.tsv line by line
		 * @Param key: the input key type
		 * @Param value: the input value type
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//The same logic as in MovieMapper
			//except that we don't flag the actor because
			//it is enough if we know if the current string is a movie or not
			//we don't need to be able to tell that a current string is an actor name
			String line = value.toString();
			String[] movie = line.split("\t");			
			
			//wikiID is always the first element
			int movieId = Integer.parseInt(movie[0]);
			//the character.metadata.tsv contains one actor per line
			//therefore we don't have to do anything special if there are multiple
			//actors, we just parse the whole file line by line
			//actor name is always the ninth element
			String actorName = movie[8];
			context.write(new IntWritable(movieId), new Text(actorName));
		}
	}

	public static class CountReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		//IntWritable: Input key type
		//Text: Input value type
		//IntWritable: Output key type
		//Text: Output value type
		
		/**
		 * Processes the values for each key (movieID) and write it into the context
		 * @Param key: The output key from the mappers
		 * @Param value: The collection of values for a single key
		 * @Param context: contains all kind of informations such as the RecordWriter which stores
		 * keyout and valout
		 */
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String stuff = new String();
			//we generate the movieFlag
			String movieFlag = "MovieFlag" + "_" + key;

			//we loop through the values for a single key
			for (Text value: values) {
				//we check if the string is flagged as movie...
				if (value.toString().contains(movieFlag)) {
					//... if yes, we create a substring which excludes the movieflag and add the rest of the already parsed
					//stuff afterwards
					//By doing this, we always hafe the movie flag at the beginning, no matter when it actually appears in the values
					stuff = value.toString().substring(0, value.toString().length() - movieFlag.length()) + "," + stuff;
				} else {
					//... if not, we add the value to the string and append a comma
					stuff = stuff + value.toString() + ",";
				}
			}
			//we have to remove the last element (it's a comma)
			String val = stuff.substring(0, stuff.length() - 1);
			context.write(key, new Text(val));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
						
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		Path movieData = new Path(args[0]+"data/movie.metadata.tsv");
		Path actorData = new Path(args[0]+"data/character.metadata.tsv");
		
		Path outputDestination = new Path(args[0] + args[1]);
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}
		
		/**
		 * Job 1
		 * Maps the movie id and movie name from movieData
		 * and the movie id and actor names from actorData
		 * and saves the result in the desired output path
		 */
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(Join.class);
		
		if (args.length > 2){
			job1.setNumReduceTasks(Integer.parseInt(args[2]));
		} else {
			job1.setNumReduceTasks(1);
		}
		
		//we don't need to set the MapperClass, since we do that in MultipleInputs
		job1.setReducerClass(CountReducer.class);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		
		//each inputPath gets its own Mapper
		MultipleInputs.addInputPath(job1, movieData, TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(job1, actorData, TextInputFormat.class, ActorMapper.class);
		FileOutputFormat.setOutputPath(job1, outputDestination);
						
		int status = job1.waitForCompletion(true) ? 0 : 1;
		
		return status;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Join(), args);
	}
}