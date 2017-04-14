package co.za.ravi.spark.core.examples;

import co.za.ravi.spark.protobuf.AddressBooks;
import co.za.ravi.spark.utils.SparkUtils;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ravikumar on 1/4/17.
 */
public class AddressBookProtobufWriter {

    public static void main(String[] args) throws  Exception  {
        AddressBookProtobufWriter addressBookProtobufWriter = new AddressBookProtobufWriter();

        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContext("AddressBookProtoBufWriter");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        LzoProtobufBlockOutputFormat.setClassConf(AddressBooks.AddressBook.class,conf);

        AddressBooks.AddressBook.Builder addressBook = AddressBooks.AddressBook.newBuilder();

        addressBook.addPerson(addressBookProtobufWriter.createPerson("Ravikumar","0844257828","ravikumar1984@gmail.com",1));
        addressBook.addPerson(addressBookProtobufWriter.createPerson("Ramesh","0844257828","ramesh1984@gmail.com",2));
        addressBook.addPerson(addressBookProtobufWriter.createPerson("Vishnu","0844257828","kmvishnuprasad@gmail.com",3));

        List<AddressBooks.AddressBook> addressBookList = new ArrayList<>();
        addressBookList.add(addressBook.build());
        JavaRDD<AddressBooks.AddressBook> addressBookRDD = javaSparkContext.parallelize(addressBookList);

        PairFunction<AddressBooks.AddressBook,IntWritable,ProtobufWritable> fileDataWithLength = new PairFunction<AddressBooks.AddressBook, IntWritable, ProtobufWritable>() {
            @Override
            public Tuple2<IntWritable, ProtobufWritable> call(AddressBooks.AddressBook s) throws Exception {
                ProtobufWritable protobufWritable = ProtobufWritable.newInstance(AddressBooks.AddressBook.class);
                protobufWritable.set(s);
                return new Tuple2<>(new IntWritable(s.getPersonCount()),protobufWritable);
            }
        };
        JavaPairRDD<IntWritable,ProtobufWritable> fileData = addressBookRDD.mapToPair(fileDataWithLength);
        fileData.saveAsNewAPIHadoopFile(args[0],IntWritable.class,ProtobufWritable.class,LzoProtobufBlockOutputFormat.class,conf);
        javaSparkContext.close();

    }

    private AddressBooks.Person createPerson(String name, String phoneNumber, String emailId, int id) {
        AddressBooks.Person.Builder builder = AddressBooks.Person.newBuilder();
        builder.setEmail(emailId)
                .setId(id)
                .setName(name);
        AddressBooks.Person.Phone.Builder phoneBuilder  = AddressBooks.Person.Phone.newBuilder();
        phoneBuilder.setPhoneNumber(phoneNumber);
        phoneBuilder.setType(AddressBooks.Person.PhoneType.MOBILE);
        builder.addPhone(phoneBuilder);
        return builder.build();
    }


}
