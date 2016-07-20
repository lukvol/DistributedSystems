package assignment3;

//Lukas Vollenweider
//13-751-888

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DecimalFormat;
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



public class AverageTime {
	
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
	 * @return double which represents the timestamp relative to the beginning in milliseconds
	 */
	private static Double parseTimestamp(String timestamp) {
		Pattern timestampPattern = Pattern.compile("(T|t)imestamp=\"(\\d)+\"");
		Matcher m = timestampPattern.matcher(timestamp);
		
		if (m.find()) {
			Pattern timePattern = Pattern.compile("\"(\\d)+");
			Matcher timeMatch = timePattern.matcher(m.group());
			if (timeMatch.find()) {
				//since the regex match we get here is in format "1873491, we need to remove
				//the first element
				String time = timeMatch.group().substring(1, timeMatch.group().length());
				return new Double(time);
			}
		}
		return 0.0;
	}
	
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("JavaAverageTime").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
				
		//get all lines which are commands
		JavaRDD<String> commands = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return isCommand(s);
			}
		});
		
		//map all timestamps to the key 1 to be able to get the last timestamp
		//by key reducing afterwards
		JavaPairRDD<Integer, Double> commandWithTimestamp = commands.mapToPair(new PairFunction<String, Integer, Double>() {
			@Override
			public Tuple2<Integer, Double> call(String s) {
				Double timestamp = parseTimestamp(s);
				return new Tuple2<Integer, Double>(1, timestamp);
			}
		});
		
		//Since all timestamps are relative to the beginning, we get the latest timestamp by
		//getting the biggest value. Therefore we check which value is the bigger one and return it
		//which results in the last timestamp for the key 1
		JavaPairRDD<Integer, Double> counts = commandWithTimestamp.reduceByKey(new Function2<Double, Double, Double>() {
			@Override
			public Double call(Double i1, Double i2) {
				if (i1 < i2) {
					return i2;
				} else {
					return i1;
				}
			}
		});
		
		List<Tuple2<Integer, Double>> timestampCombined = counts.collect();
		
		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));
		Double endTimestamp = timestampCombined.get(0)._2();

		Double result = endTimestamp / commands.count() ;
				
		DecimalFormat df = new DecimalFormat("#.##");      
		result = Double.valueOf(df.format(result));
		
		outputBW.write(Double.toString(result));
		outputBW.close();
		ctx.close();
	}
}
