package co.za.ravi.spark.core.examples;

import co.za.ravi.spark.utils.SparkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;


/**
 * Created by ravikumar on 1/7/17.
 */
public class ElasticSearch {

    public static void main(String[] args) throws  Exception {
        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContext("ElasticSearch");
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf);
        jobConf.setOutputCommitter(FileOutputCommitter.class);
        jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, args[0]);
        jobConf.set(ConfigurationOptions.ES_NODES,"localhost");
        FileOutputFormat.setOutputPath(jobConf, new Path("-"));
        jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat");

        JavaPairRDD data = javaSparkContext.hadoopRDD(jobConf, EsInputFormat.class, NullWritable.class, MapWritable.class);

    }
}
