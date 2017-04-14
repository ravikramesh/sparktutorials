package co.za.ravi.spark.core.examples;

import co.za.ravi.spark.utils.SparkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by ravikumar on 1/1/17.
 */
public class Aggregate implements Serializable {

    private  static  final Logger logger = Logger.getLogger(Aggregate.class);

    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContext("Aggregate");
        Aggregate aggregate = new Aggregate();
        logger.warn(aggregate.average(javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8))));
        Thread.sleep(1*1000);
        aggregate.averageUsingJavaDouble(javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8)));
        aggregate.checkCombineByKey(javaSparkContext);
    }

    Function2<AverageCount,Integer, AverageCount> addAndCount = new Function2<AverageCount, Integer, AverageCount>() {
        @Override
        public AverageCount call(AverageCount v1, Integer v2) throws Exception {
            v1.total += v2;
            v1.number +=1;
            return v1;
        }
    };

    Function2<AverageCount,AverageCount, AverageCount> combine = new Function2<AverageCount, AverageCount, AverageCount>() {
        @Override
        public AverageCount call(AverageCount v1, AverageCount v2) throws Exception {
            v1.total += v2.total;
            v1.number += v2.number;
            return v1;
        }
    };

    Function<Integer,AverageCount> createAcc = new Function<Integer, AverageCount>() {
        @Override
        public AverageCount call(Integer v1) throws Exception {
            return new AverageCount(v1,1);
        }
    };


    private void checkCombineByKey(JavaSparkContext javaSparkContext){
//        JavaPairRDD<String,Integer> words= javaSparkContext.parallelizePairs(setUpTestData());
        JavaPairRDD<String,Integer> words= javaSparkContext.parallelizePairs(setUpTestData()).partitionBy(new StringKeyPartitioner(3));
        for (Partition partition : words.partitions()) {
                logger.warn(partition.toString());
        }
        JavaPairRDD<String, AverageCount> avgCounts = words.combineByKey(createAcc, addAndCount, combine);
        for (Map.Entry<String, AverageCount> avgCountEntry : avgCounts.collectAsMap().entrySet()) {
            logger.warn(" Key :" + avgCountEntry.getKey()+ " -> avg : "+avgCountEntry.getValue().avg());
        }

    }



    private void squaring(JavaRDD<Integer> inputRDD) {
        JavaRDD<Double> mapsRDD = inputRDD.map(new Function<Integer, Double>() {
            @Override
            public Double call(Integer v1) throws Exception {
                return v1*v1*1.0;
            }
        });
        logger.warn(StringUtils.join(mapsRDD.collect(),","));
    }

    private void averageUsingJavaDouble(JavaRDD<Integer> inputRDD) {
        JavaDoubleRDD mapsRDD = inputRDD.mapToDouble(new DoubleFunction<Integer>() {
            @Override
            public double call(Integer integer) throws Exception {
                return integer*1.0;
            }
        });
        logger.warn("Mean "+mapsRDD.mean());
    }

    public double average(JavaRDD<Integer> inputRDD) {
        AverageCount initialize = new AverageCount(0,0);
        AverageCount averageCount = inputRDD.aggregate(initialize,addAndCount,combine);
        return averageCount.avg();

    }

    private List<Tuple2<String,Integer>> setUpTestData(){
        List<Tuple2<String,Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("Ravi",12));
        data.add(new Tuple2<>("Vishnu",32));
        data.add(new Tuple2<>("Ravi",8));
        data.add(new Tuple2<>("Vishnu",67));
        data.add(new Tuple2<>("Vishnu",27));
        data.add(new Tuple2<>("Ravi",3));
        data.add(new Tuple2<>("Ramesh",55));
        data.add(new Tuple2<>("Deepthi",56));
        data.add(new Tuple2<>("Ramesh",85));
        data.add(new Tuple2<>("Vishnu",1));
        data.add(new Tuple2<>("Ramesh",5));

        data.add(new Tuple2<>("Ravi",2));
        data.add(new Tuple2<>("Ramesh",39));
        data.add(new Tuple2<>("Deepthi",123));
        data.add(new Tuple2<>("Deepthi",78));

        data.add(new Tuple2<>("Deepthi",3));
        data.add(new Tuple2<>("Vishnu",78));
        data.add(new Tuple2<>("Vishnu",7));
        data.add(new Tuple2<>("Vishnu",3));

        data.add(new Tuple2<>("Deepthi",91));
        data.add(new Tuple2<>("Ramesh",44));
        data.add(new Tuple2<>("Deepthi",45));
        data.add(new Tuple2<>("Ravi",50));
        data.add(new Tuple2<>("Ravi",5));
        return data;
    }

    class AverageCount implements  Serializable {
        private int total;
        private int number;

        public AverageCount(int total, int number) {
            this.total = total;
            this.number = number;
        }

        public double avg() {
            logger.warn(" total: "+ total+ " number:"+ number + " avg:"+(total*1.0/number));
            return total*1.0/number;
        }
    }

    class StringKeyPartitioner extends Partitioner {
        private int numberofPartitioner;

        public StringKeyPartitioner(int numberofPartitioner) {
            this.numberofPartitioner = numberofPartitioner;
        }

        @Override
        public int numPartitions() {
            return numberofPartitioner;
        }

        @Override
        public int getPartition(Object key) {
            String aggregateKey = (String) key;
            int code = aggregateKey.hashCode() % numberofPartitioner;
            return code < 0 ? (code+numberofPartitioner) : code;
        }
    }
}
