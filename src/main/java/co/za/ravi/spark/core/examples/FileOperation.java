package co.za.ravi.spark.core.examples;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import co.za.ravi.spark.core.examples.dto.Person;
import co.za.ravi.spark.utils.SparkUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by ravikumar on 1/2/17.
 */
public class FileOperation implements  Serializable {
    private static final  Logger  LOGGER = Logger.getLogger(FileOperation.class);

    public static void main(String[] args) {
        FileOperation fileOperation = new FileOperation();
        switch (args[0]) {
            case "json" :
                fileOperation.saveJsonAsFile(fileOperation.loadPersonJson(),args[1]);
                return;
            case "csv":
                fileOperation.saveCSVFile(fileOperation.parseCSVFile(),args[1]);
                return;
            case "hadoop":
                fileOperation.saveAsSequenceFile(args[1]);
                return;
            case "object":
                fileOperation.saveAsObjectFile(args[1]);
                return;
            default:
                LOGGER.warn("Invalid option : Usage  FileOperation option outputPath. Option [ json / csv/ hadoop/ object]");
                return;
        }
    }

    private JavaRDD<Person> loadPersonJson(){
        JavaRDD<String>  jsonInput = SparkUtils.getJavaSparkContext("FileOperation").textFile("input/person.json");
        return jsonInput.mapPartitions(new ParseJson());
    }

    private void saveJsonAsFile(JavaRDD<Person> persons, String outputFilePath) {
        JavaRDD<String> jsonText = persons.mapPartitions(new WriteJson());
        jsonText.saveAsTextFile(outputFilePath);
    }

    private JavaRDD<String[]> parseCSVFile() {
        JavaRDD<String>  csvFile = SparkUtils.getJavaSparkContext("FileOperation").textFile("input/stats_by_zip.csv");
        return csvFile.map(new ParseLine());
    }

    private void saveCSVFile(JavaRDD<String[]> dataLines, String outputFilePath) {
        JavaRDD<String> jsonText = dataLines.map(new WriteCSVFile());
        jsonText.saveAsTextFile(outputFilePath);
    }

    private void saveAsSequenceFile(String outputFilePath) {
        JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext("SaveAsSequenceFile");

        JavaRDD<String> inputFile = sparkContext.textFile("input/README.txt");
        PairFunction<String,IntWritable,Text> fileDataWithLength = new PairFunction<String, IntWritable, Text>() {
            @Override
            public Tuple2<IntWritable, Text> call(String s) throws Exception {
                return new Tuple2<>(new IntWritable(s.length()),new Text(s));
            }
        };
        JavaPairRDD<IntWritable,Text> fileData = inputFile.mapToPair(fileDataWithLength);

        fileData.saveAsHadoopFile(outputFilePath, IntWritable.class, Text.class, SequenceFileOutputFormat.class);
    }

    private void saveAsObjectFile(String outputFilePath) {
        JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext("SaveAsObjectFile");
        JavaRDD<String> inputFile = sparkContext.textFile("input/README.txt");
        inputFile.saveAsObjectFile(outputFilePath);
    }

    class ParseLine implements Function<String,String[]>, Serializable{

        @Override
        public String[] call(String line) throws Exception {
            CSVReader csvReader = new CSVReader(new StringReader(line));
            return csvReader.readNext();
        }
    }

    class WriteCSVFile implements Function<String[], String>, Serializable {

        @Override
        public String call(String[] data) throws Exception {
            StringWriter stringWriter = new StringWriter();
            CSVWriter csvWriter = new CSVWriter(stringWriter);
            csvWriter.writeNext(data);
            return stringWriter.toString();
        }
    }

    class ParseJson implements FlatMapFunction<Iterator<String>, Person>, Serializable {

        @Override
        public Iterator<Person> call(Iterator<String> input) throws Exception {
            ArrayList<Person> personList = new ArrayList<>();
            ObjectMapper mapper = new ObjectMapper();
            while (input.hasNext()){
                try {
                    personList.add(mapper.readValue(input.next(), Person.class));
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return personList.iterator();
        }
    }

    class WriteJson implements FlatMapFunction<Iterator<Person>,String> {

        @Override
        public Iterator<String> call(Iterator<Person> persons) throws Exception {
            ArrayList<String > text = new ArrayList<>();
            ObjectMapper mapper = new ObjectMapper();
            while (persons.hasNext()){
                try {
                    text.add(mapper.writeValueAsString(persons.next()));
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return text.iterator();
        }
    }
}
