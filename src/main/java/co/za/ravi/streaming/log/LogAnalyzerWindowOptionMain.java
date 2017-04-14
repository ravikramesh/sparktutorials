package co.za.ravi.streaming.log;

import co.za.ravi.spark.utils.SparkUtils;
import co.za.ravi.streaming.log.Functions.AddLongs;
import co.za.ravi.streaming.log.Functions.SubtractLongs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


/**
 * Created by ravikumar on 1/25/17.
 */
public class LogAnalyzerWindowOptionMain {

    private static final Logger LOGGER = Logger.getLogger(LogAnalyzerWindowOptionMain.class);
    public static final String CHECKPOINT_DIR = "/user/ravikumar/sparkcheckpoint";
    public static final String OUTPUT_DIR = "test12";
    public static final String FILENAME_PREFIX = "ipcount1";
    public static final String HOSTNAME = "localhost";
    public static final int PORT = 7777;

    public static void main(String[] args) throws Exception {
        JavaSparkContext jsc = SparkUtils.getJavaSparkContext("Log Analyze");
        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(10));
      //  getSocketStream(ssc);
        getTextFileStream(ssc,args[0]);
    }

    private static void getTextFileStream(JavaStreamingContext ssc,String logDirectory) throws InterruptedException {
        JavaDStream<String> textFileStream = ssc.textFileStream(logDirectory);
        JavaDStream<ApacheAccessLog> accessLogJavaDStream = textFileStream.map(new Functions.ParseLogLine());
        JavaDStream<ApacheAccessLog> windowStream = accessLogJavaDStream.window(Durations.seconds(20), Durations.seconds(10));
        JavaPairDStream<String, Long> ipStream = accessLogJavaDStream.mapToPair(new Functions.IpTuple());
        ipStream.print();
        LOGGER.error(" Going to print stats :");
        windowStream.print();
        ssc.start();
        ssc.awaitTermination();
    }

    private static void getSocketStream(JavaStreamingContext ssc) throws InterruptedException {
        JavaReceiverInputDStream<String> inputDStream = ssc.socketTextStream(HOSTNAME, PORT);

        JavaDStream<ApacheAccessLog> accessLogJavaDStream = inputDStream.map(new Functions.ParseLogLine());

        processAccessLogStream(OUTPUT_DIR,accessLogJavaDStream);
        ssc.checkpoint(CHECKPOINT_DIR);
        ssc.start();
        ssc.awaitTermination();
    }

    private static void processAccessLogStream(String outputDir, JavaDStream<ApacheAccessLog> accessLogJavaDStream ) {
        JavaDStream<ApacheAccessLog> windowStream = accessLogJavaDStream.window(Durations.seconds(3), Durations.seconds(1));
        JavaPairDStream<String, Long> ipStream = accessLogJavaDStream.mapToPair(new Functions.IpTuple());
        ipStream.print();
        JavaPairDStream<String, Long> ipAddressCountDStream = ipStream.reduceByKeyAndWindow(new AddLongs(), new SubtractLongs(), Durations.seconds(3), Durations.seconds(1));
        LOGGER.info(" ipAddressCount :: ");
        ipAddressCountDStream.print();
        JavaDStream<String> ip = accessLogJavaDStream.map(new Function<ApacheAccessLog, String>() {
            @Override
            public String call(ApacheAccessLog v1) throws Exception {
                return v1.getIpAddress();
            }
        });

        JavaDStream<Long> requestCount = accessLogJavaDStream.countByWindow(Durations.seconds(3), Durations.seconds(1));
        LOGGER.info(" requestCount :");
        requestCount.print();
        JavaPairDStream<String, Long> ipAddressRequestCount = ip.countByValueAndWindow(Durations.seconds(3), Durations.seconds(1));
        LOGGER.info(" ipAddressRequestCount :");
        ipAddressRequestCount.print();
        JavaPairDStream<Text, LongWritable> writableJavaPairDStream = ipAddressRequestCount.mapToPair(new PairFunction<Tuple2<String, Long>, Text, LongWritable>() {
            @Override
            public Tuple2<Text, LongWritable> call(Tuple2<String, Long> input) throws Exception {
                return new Tuple2<Text, LongWritable>(new Text(input._1()), new LongWritable(input._2()));
            }
        });
        class OutFormat extends TextOutputFormat<Text,LongWritable> {
        //class OutFormat extends SequenceFileOutputFormat<Text,LongWritable> {

        }
        writableJavaPairDStream.saveAsHadoopFiles(FILENAME_PREFIX,"txt",Text.class, LongWritable.class, OutFormat.class);
    }


}
