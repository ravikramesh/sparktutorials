package co.za.ravi.streaming.log;

import co.za.ravi.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by ravikumar on 1/15/17.
 */
public class LogAnalyzerMain implements Serializable {
    public static void main(String[] args) throws Exception {
        JavaSparkContext jsc = SparkUtils.getJavaSparkContext("Log Analyze");
        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(1));
        JavaReceiverInputDStream<String> inputDStream = ssc.socketTextStream("localhost", 7777);

        printErrorFilterStream(inputDStream,ssc);

        JavaDStream<ApacheAccessLog> accessLogJavaDStream = inputDStream.map(new Functions.ParseLogLine());
        JavaPairDStream<String, Long> ipCountsStream  = getIpCountsFromLogs(accessLogJavaDStream);
        JavaPairDStream<String, Long> ipBytesSizeSumStream  = getIpBytesSumFromLogs(accessLogJavaDStream);
        JavaPairDStream<String, Tuple2<Long, Long>> ipCountsBytesSumStream = ipCountsStream.join(ipBytesSizeSumStream);
        ipCountsBytesSumStream.print();

        ssc.start();
        ssc.awaitTermination();
    }

    private static JavaPairDStream<String, Long>  getIpCountsFromLogs(JavaDStream<ApacheAccessLog> accessLogJavaDStream) {
        JavaPairDStream<String, Long> ipStream = accessLogJavaDStream.mapToPair(new Functions.IpTuple());
        return ipStream.reduceByKey(new Functions.LongSumReducer());
    }

    private static JavaPairDStream<String, Long>  getIpBytesSumFromLogs(JavaDStream<ApacheAccessLog> accessLogJavaDStream) {
        JavaPairDStream<String, Long> ipStream = accessLogJavaDStream.mapToPair(new Functions.IpContentsTuple());
        return ipStream.reduceByKey(new Functions.LongSumReducer());
    }

    private static void printErrorFilterStream(JavaReceiverInputDStream<String> inputDStream, JavaStreamingContext ssc) throws InterruptedException {
        JavaDStream<String> errorStream = inputDStream.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                return line.contains("error");
            }
        });
        errorStream.print();

    }

}
