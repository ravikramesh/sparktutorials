package co.za.ravi.spark.core.examples;

import co.za.ravi.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ravikumar on 12/30/16.
 */
public class WordCount {

    public static void main(String[] args) {
        WordCount wordCount = new WordCount();
        wordCount.countWord(args[0], args[1]);
    }

    private void countWord(String inputFilePath, String outputFilePath) {
        JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext("WordCount");

        JavaRDD<String> inputFile = sparkContext.textFile(inputFilePath);
        JavaRDD<String> words = inputFile.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordCountRDD = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1))
                                                          .reduceByKey((v1, v2) -> v1+v2);
        wordCountRDD.saveAsTextFile(outputFilePath);

    }

    private void finByWords(String inputFilePath, String outputFilePath) {
        JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext("WordCount");

        JavaRDD<String> inputFile = sparkContext.textFile(inputFilePath);
        JavaRDD<String> isWordsRDD = inputFile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.contains("is");
            }
        });

        JavaRDD<String> licenseWordRDD = inputFile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.toLowerCase().contains("license");
            }
        });

        JavaRDD<String> combinedRDD = isWordsRDD.union(licenseWordRDD);

        System.out.println(" Searched word count "+ combinedRDD.count());

        for (String word : combinedRDD.take(10)) {
            System.out.println(" filtered words :" + word);
        }
        combinedRDD.saveAsTextFile(outputFilePath);
    }

}
