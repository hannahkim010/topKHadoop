package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, FloatWritable> {

	// Create a counter and initialize with 1
	private final FloatWritable counter = new FloatWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();
	private boolean isHeader = true;

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		// code for task 1
		// String[] result = value.toString().split(",");
		// word.set(result[7]);
		// context.write(word, counter);

		// convert value to String to process header
		String line = value.toString();
		
		// if line is header, skip 
		if (isHeader(line)) {
			return;
		}

		// split vals into diff attribute fields
		String[] values = line.split(",");

		// airline input
		word.set(values[4]);
		int delayAmt = 0;
		try {
			// flight_delay input
			delayAmt = Integer.parseInt(values[11]);
		} catch (NumberFormatException e) {
			// case of no data, do nothing
		}

		// set counter to the delay
		counter.set(delayAmt);
		// set in map the key = airline, value = delayAmt
		context.write(word, counter);
	}

	// helper method to identify header lines
	private boolean isHeader(String line) {
		return line.startsWith("YEAR,MONTH,DAY,DAY_OF_WEEK,AIRLINE,FLIGHT_NUMBER,TAIL_NUMBER,ORIGIN_AIRPORT,DESTINATION_AIRPORT,SCHEDULED_DEPARTURE,DEPARTURE_TIME,DEPARTURE_DELAY,TAXI_OUT,WHEELS_OFF,SCHEDULED_TIME,ELAPSED_TIME,AIR_TIME,DISTANCE,WHEELS_ON,TAXI_IN,SCHEDULED_ARRIVAL,ARRIVAL_TIME,ARRIVAL_DELAY,DIVERTED,CANCELLED,CANCELLATION_REASON,AIR_SYSTEM_DELAY,SECURITY_DELAY,AIRLINE_DELAY,LATE_AIRCRAFT_DELAY,WEATHER_DELAY");
	}
}
