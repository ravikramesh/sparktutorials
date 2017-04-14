package co.za.ravi.spark.core.examples;

import co.za.ravi.spark.utils.SparkUtils;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by ravikumar on 1/8/17.
 */
public class TemperatureAccumulator implements Serializable {

    private static Logger LOGGER = Logger.getLogger(TemperatureAccumulator.class);

    public static void main(String[] args) {
        TemperatureAccumulator temperatureAccumulator = new TemperatureAccumulator();
        temperatureAccumulator.readTemperatureFile(args[0]);
    }

    private void readTemperatureFile(String outputfile) {
        JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext("TemperatureAccumulator");
         final Accumulator<Integer> inValidRecordCounter = sparkContext.accumulator(0);
         final Accumulator<Integer> validRecordCounter = sparkContext.accumulator(0);
        JavaRDD<String> inputFile = sparkContext.textFile("input/029070-99999-1901");
        final Integer MISSING = 9999;
        JavaPairRDD<String, Integer> temperatureRdd = inputFile.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String record) throws Exception {
                String year = record.substring(15,19);
                int airTemperature ;
                if(record.charAt(87) == '+') {
                    airTemperature = Integer.valueOf(record.substring(88,92));
                } else {
                    airTemperature = Integer.valueOf(record.substring(88,92));
                }
                String quality = record.substring(92,93);
                if(airTemperature != MISSING && quality.matches("[012345]")){
                    validRecordCounter.add(1);
                    return  new Tuple2<String, Integer>(year,airTemperature);
                }
                inValidRecordCounter.add(1);
                return null;
            }
        });
        temperatureRdd.saveAsTextFile(outputfile);
        LOGGER.warn(" invalid Record count : "+inValidRecordCounter.toString());
        LOGGER.warn(" valid Record count : "+ validRecordCounter.toString());
    }
}
