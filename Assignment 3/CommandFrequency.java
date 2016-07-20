package assignment3;

//Lukas Vollenweider
//13-751-888

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CommandFrequency {
	
	/**
	 * checks if a line is a command
	 * @param line line to check
	 * @return returns true if a line is a command and false otherwise
	 */
	private static Boolean isCommand(String line) {
		Pattern commandPattern = Pattern.compile("<Command.*(/>)?");
		Matcher m = commandPattern.matcher(line);
		if (m.find()) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Gets the id from a command. If the command is an EclipseCommand, it gets the commandID
	 * @pre command has to be a line which contains a command
	 * @pre pattern has to be a pattern which gets the commandID
	 * @param command string which represents a command
	 * @param pattern pattern to get a commandID
	 * @return command id
	 */
	private static String getIdFromCommand(String command, String pattern) {
		Pattern idPattern = Pattern.compile(pattern);
		Matcher m = idPattern.matcher(command);
		if (m.find()) {
			if (m.group().contains("Eclipse")) {
				return "EclipseCommand:" + getIdFromCommand(command, "commandID=\".*\"");
			} else {
				Pattern valueP = Pattern.compile("\"(\\w+(\\.)?)+");
				Matcher value = valueP.matcher(m.group());
				if (value.find()) {
					String commandId = value.group().substring(1, value.group().length());
					return commandId;
				}
			}
		} 
		return "";
	}
	
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("JavaCommandFrequency").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		
		//get all the lines which are a command
		JavaRDD<String> commands = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return isCommand(s);
			}
		});
		
		//for each command, we get the id and save it as key and map 1 to it
		//to be able to add up all values for a single key
		JavaPairRDD<String, Integer> commandIDWithCommand = commands.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(getIdFromCommand(s, "_type=\".+\""), 1);
			}
		});
		
		//add all values for a single key to get the amount of commands for a commandID
		JavaPairRDD<String, Integer> counts = commandIDWithCommand.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		//swap key with value
		JavaPairRDD<Integer, String> swappedPair = counts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
						return item.swap();
					}

				});
				
		//sortByKey(ascending?)
		JavaPairRDD<Integer, String> swappedPairSorted = swappedPair.sortByKey(false);
		
		//swap back after sorting
		JavaPairRDD<String, Integer> swappedPairSortedSwapBack = swappedPairSorted
				.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
						return item.swap();
					}

				});

		List<Tuple2<String, Integer>> output = swappedPairSortedSwapBack.collect();

		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));

		for (Tuple2<?, ?> tuple : output) {
			outputBW.write(tuple._1() + "\t" + tuple._2() +"\n");
		}
		outputBW.close();
		ctx.stop();
	}
}
