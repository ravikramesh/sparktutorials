package co.za.ravi.sparksql;

import co.za.ravi.spark.core.examples.dto.Person;
import co.za.ravi.spark.utils.SparkUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ravikumar on 1/13/17.
 */
public class HiveSQLQueryOperation {
    private static Logger LOGGER = Logger.getLogger(HiveSQLQueryOperation.class);
    public static void main(String[] args) {
        readFromHiveTable();
        readRDDUsingHiveTempTable();
    }

    private static void readFromHiveTable() {
        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContext("HiveSQLQueryOperation");
        HiveContext hiveContext = new HiveContext(javaSparkContext);
        Dataset<Row> rowDataset = hiveContext.sql("SELECT id, name from testing");
        JavaRDD<String> names = rowDataset.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(1);
            }
        });
        LOGGER.warn(" data records " +names.collect());
    }

    private static void readRDDUsingHiveTempTable() {
        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContext("HiveSQLQueryOperation");
        HiveContext hiveContext = new HiveContext(javaSparkContext);
        List<Person> personList = getPersonList();
        JavaRDD<Person> personJavaRDD = javaSparkContext.parallelize(personList);
        Dataset<Row> personRowSet = hiveContext.applySchema(personJavaRDD, Person.class);
        personRowSet.registerTempTable("person");
        Dataset<Row> rowDataset = hiveContext.sql("SELECT name from person");
        LOGGER.warn(" Row Count "+rowDataset.count());
    }

    private static List<Person> getPersonList() {
        List<Person> personList = new ArrayList<>();
        personList.add(createPerson("Ravikumar","Ramasamy","19-05-1984","M"));
        personList.add(createPerson("Ramesh","Ramasamy","19-05-1984","M"));
        return personList;
    }

    private static  Person createPerson(String name, String fatherName, String dob, String gender){
        Person person = new Person();
        person.setName(name);
        person.setFatherName(fatherName);
        person.setCountry("India");
        person.setDob(dob);
        person.setGender(gender);
        return person;
    }
}
