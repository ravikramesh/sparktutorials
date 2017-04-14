package co.za.ravi.spark.core.examples;

import co.za.ravi.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by ravikumar on 1/1/17.
 */
public class FilterOpetaionOnPairRDD {
    public static void main(String[] args) {
        FilterOpetaionOnPairRDD filterOpetaionOnPairRDD = new FilterOpetaionOnPairRDD();
        JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext("PairRDD");

        JavaRDD<String> inputFile = sparkContext.textFile(args[0]);
        filterOpetaionOnPairRDD.filterLongWord(inputFile,args[1]);
    }

    private void filterLongWord(JavaRDD<String> inputFile, String outputFilePath) {


        PairFunction<String,String,String> keyData = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0],s);
            }
        };
        JavaPairRDD<String,String> firstWordKeyMap = inputFile.mapToPair(keyData);

        Function<Tuple2<String,String>, Boolean> longwordFilter = new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                return v1._1().length() > 15;
            }
        };

        JavaPairRDD<String,String> filteredData = firstWordKeyMap.filter(longwordFilter);
        filteredData.saveAsTextFile(outputFilePath);
    }
}
