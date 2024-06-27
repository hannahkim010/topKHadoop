package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

   public void reduce(Text text, Iterable<FloatWritable> values, Context context)
           throws IOException, InterruptedException {
	
    //    task 1 code:
    //    int sum = 0;
       
    //    for (FloatWritable value : values) {
    //        sum += value.get();
    //    }
       
    //    context.write(text, new FloatWritable(sum));

        // task 2 code:

        // adjust to float 
        float totalDelay = 0;
        int flightCounter = 0;
        
        // for each map-key val for airline, sum up total delayAmts
        for (FloatWritable value : values) {
            totalDelay += value.get();
            // increment flight counter
            flightCounter += 1;
        }

        // calculates avg delay ratio given all the values
        float avgDelayRatio = flightCounter > 0 ? totalDelay / flightCounter : 0;

        // write in the delay ratio
        // save airline with the found delay ratio
        context.write(text, new FloatWritable(avgDelayRatio));
   }
}
