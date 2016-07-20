package assignment3;

//Lukas Vollenweider
//13-751-888

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CommandCount {
	
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
	 * Gets the timestamp from a command and converts it to a double
	 * @param timestamp command line
	 * @return long which represents the timestamp relative to the beginning in milliseconds
	 */
	private static Long parseTimestamp(String timestamp) {
		Pattern timestampPattern = Pattern.compile("(T|t)imestamp=\"(\\d)+\"");
		Matcher m = timestampPattern.matcher(timestamp);
		
		if (m.find()) {
			Pattern timePattern = Pattern.compile("\"(\\d)+");
			Matcher timeMatch = timePattern.matcher(m.group());
			if (timeMatch.find()) {
				String time = timeMatch.group().substring(1, timeMatch.group().length());
				return new Long(time);
			}
		}
		return (long) 0;
	}
	
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("JavaCommandCount").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);		
		
		//get all the lines which are commands
		JavaRDD<String> commands = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return isCommand(s);
			}
		});
		
		//since we want to know the commands per 15 minutes, we convert the timestamp to minutes and
		//divide the result by 15. The result is then used as the new key. As value we use a 1 which
		//represents the amount of commands
		JavaPairRDD<Long, Integer> timestampPer15 = commands.mapToPair(new PairFunction<String, Long, Integer>() {
			@Override
			public Tuple2<Long, Integer> call(String s) {
				long timestamp = parseTimestamp(s);
				return new Tuple2<Long, Integer>(TimeUnit.MILLISECONDS.toMinutes(timestamp) / 15, 1);
			}
		});
		
		//we add up the values for each timestamp to get the amount of commands per key
		JavaPairRDD<Long, Integer> counts = timestampPer15.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		JavaPairRDD<Long, Integer> pairSorted = counts.sortByKey(true);
		
		List<Tuple2<Long, Integer>> output = pairSorted.collect();

		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));
		
		for (Tuple2<?, ?> tuple : output) {
			outputBW.write(tuple._1() + "\t" + tuple._2() +"\n");
		}
		outputBW.close();
		ctx.stop();
	}
}
