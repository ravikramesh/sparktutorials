package co.za.ravi.spark.core.examples;

import co.za.ravi.spark.utils.SparkUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

import java.io.Serializable;

/**
 * Created by ravikumar on 1/8/17.
 */
public class RunProcess implements Serializable {

    private static final Logger LOGGER = Logger.getLogger(RunProcess.class);

    public static void main(String[] args) {
        RunProcess runProcess = new RunProcess();
        runProcess.findDistanceUsingRProgram();
    }

    private void findDistanceUsingRProgram(){
        JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext("RunProcess");
        JavaRDD<String> inputFile = sparkContext.textFile("input/distance.txt");

        String distScript = "./src/R/finddistance.R";
        String distScriptName = "finddistance.R";
        sparkContext.addFile(distScript);

        JavaRDD<String> distances = inputFile.pipe(SparkFiles.get(distScriptName));
        LOGGER.warn(" calculated distances : "+ StringUtils.join(",",distances.collect()));

        JavaDoubleRDD distancesInDouble = distances.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String s) throws Exception {
                return Double.parseDouble(s);
            }
        });

        StatCounter stats = distancesInDouble.stats();
        double stdev = stats.stdev();
        double mean = stats.mean();
        LOGGER.warn(" statDeviation : "+ stdev + " mean: "+ mean);
        JavaDoubleRDD reasonableDistances = distancesInDouble.filter(new Function<Double, Boolean>() {
            @Override
            public Boolean call(Double v1) throws Exception {
                return Math.abs(v1-mean) < 3*stdev;
            }
        });
        LOGGER.warn(" reasonable distances : "+ StringUtils.join(",",reasonableDistances.collect()));

    }
}
