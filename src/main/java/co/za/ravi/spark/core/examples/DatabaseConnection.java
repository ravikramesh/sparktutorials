package co.za.ravi.spark.core.examples;

import co.za.ravi.spark.core.examples.dto.DBConnectionParams;
import co.za.ravi.spark.core.examples.dto.Person;
import co.za.ravi.spark.utils.SparkUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.*;
import scala.Function1;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by ravikumar on 1/7/17.
 */
public class DatabaseConnection implements  Serializable {

    private static Logger LOGGER = Logger.getLogger(DatabaseConnection.class);
    public static final String MYSQL_DB_URL = "jdbc:mysql://localhost:3306/hadoop_examples";
    public static final String MYSQL_USERNAME = "root";
    public static final String MYSQL_PASSWORD = "ramesh";
    public static final String MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver";

    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContext("DatabaseConnection");
        DatabaseConnection databaseConnection = new DatabaseConnection();
        databaseConnection.fetchDataFromDB(javaSparkContext);
        databaseConnection.writeToDatabase(javaSparkContext);
    }

    private void fetchDataFromDB(JavaSparkContext javaSparkContext) {
        DBConnectionParams dbConnectionParams = new DBConnectionParams(MYSQL_DB_URL, MYSQL_JDBC_DRIVER,MYSQL_USERNAME,MYSQL_PASSWORD);

        //Fetch data from MySQL Database.
        JdbcRDD<Map> jdbcRDD = new JdbcRDD(javaSparkContext.sc(), dbConnectionParams,"select * from employee where empId >= ? and empId < ?",1,10,1,new MapEmployeeRowData(), ClassManifestFactory$.MODULE$.fromClass(Map.class));

        JavaRDD<Map> javaRDD = JavaRDD.fromRDD(jdbcRDD,ClassManifestFactory$.MODULE$.fromClass(Map.class));
        List<String> employeeList = javaRDD.map(new Function<Map, String>() {
            @Override
            public String call(Map v1) throws Exception {
                return v1.get("empId")+" "+ v1.get("name")+ " "+v1.get("surname");
            }
        }).collect();

        for (String employee : employeeList) {
            LOGGER.warn(employee);
        }
    }

    private void writeToDatabase(JavaSparkContext javaSparkContext) {
        SparkSession  session =SparkSession.builder().sparkContext(javaSparkContext.sc()).getOrCreate();
        Dataset<Row> personList = session.createDataFrame(getPersonList(), Person.class);
        Properties properties = new Properties();
        properties.setProperty("user",MYSQL_USERNAME);
        properties.setProperty("password",MYSQL_PASSWORD);
        personList.write().mode(SaveMode.Append).jdbc(MYSQL_DB_URL,"person",properties);

    }

    private List<Person> getPersonList() {
        List<Person> personList = new ArrayList<>();
        personList.add(createPerson("Ravikumar","Ramasamy","19-05-1984","M"));
        personList.add(createPerson("Ramesh","Ramasamy","19-05-1984","M"));
        return personList;
    }

    private Person createPerson(String name, String fatherName, String dob, String gender){
        Person person = new Person();
        person.setName(name);
        person.setFatherName(fatherName);
        person.setCountry("India");
        person.setDob(dob);
        person.setGender(gender);
        return person;
    }

    private class MapEmployeeRowData extends AbstractFunction1<ResultSet,Map> implements Serializable {
        @Override
        public Map apply(ResultSet rs) {
            java.util.Map<String,String> employee = new HashMap();
            try {
                employee.put("empId",rs.getString(1));
                employee.put("name",rs.getString(2));
                employee.put("surname",rs.getString(3));
            } catch (SQLException e) {
                LOGGER.warn("Error when retrieve data from ResultSet :",e);
            }
            return employee;
        }

        @Override
        public <A> Function1<A, Map> compose(Function1<A, ResultSet> g) {
            return super.compose(g);
        }

        @Override
        public <A> Function1<ResultSet, A> andThen(Function1<Map, A> g) {
            return super.andThen(g);
        }
    }
}
