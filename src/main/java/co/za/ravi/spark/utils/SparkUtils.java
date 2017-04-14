package co.za.ravi.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ravikumar on 1/2/17.
 */
public class SparkUtils {

    public static void main(String[] args) {
        SparkUtils sparkUtils = new SparkUtils();
        sparkUtils.createJSOnFile();
    }

    public static JavaSparkContext getJavaSparkContext(String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        return new JavaSparkContext(sparkConf);
    }

    private JSONObject createJSONObject(String name, String fatherName, String country, String dateOfBirth, String gender) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name",name);
        jsonObject.put("fatherName",fatherName);
        jsonObject.put("country",country);
        jsonObject.put("dob",dateOfBirth);
        jsonObject.put("gender",gender);
        return jsonObject;
    }

    private void createJSOnFile(){
        JSONArray jsonArray = new JSONArray();
        List<JSONObject> personalDetailsList = new ArrayList<>();
        jsonArray.put(createJSONObject("Ravikumar","Ramasamy","SouthAfrica","19-05-1984","M"));
        jsonArray.put(createJSONObject("Ramesh","Ramasamy","USA","19-05-1984","M"));
        jsonArray.put(createJSONObject("Deepthi","Murugesan","India","19-05-2000","M"));
        jsonArray.put(createJSONObject("Vishnu","Murugesan","India","10-12-2002","F"));
        //jsonArray.put(personalDetailsList);
        System.out.println(jsonArray.toString());
    }
}
