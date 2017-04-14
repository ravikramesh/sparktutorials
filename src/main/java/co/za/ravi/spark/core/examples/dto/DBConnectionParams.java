package co.za.ravi.spark.core.examples.dto;

import org.apache.log4j.Logger;
import scala.runtime.AbstractFunction0;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by ravikumar on 1/7/17.
 */
public class DBConnectionParams extends AbstractFunction0<Connection> implements Serializable {
    private String dbUrl;
    private String driverClassName;
    private String userName;
    private String password;
    private static Logger LOGGER = Logger.getLogger(DBConnectionParams.class);

    public DBConnectionParams(String dbUrl, String driverClassName, String userName, String password) {
        this.dbUrl = dbUrl;
        this.driverClassName = driverClassName;
        this.userName = userName;
        this.password = password;
    }


    @Override
    public Connection apply(){
        try{
            Class.forName(driverClassName);
        }catch (Exception exp){
            LOGGER.warn(" Error when loading driver class:",exp);
            return null;
        }
        try {
            return DriverManager.getConnection(dbUrl,userName,password);
        }catch (SQLException sqlExp) {
            LOGGER.warn(" Error when creating connection: ",sqlExp);
        }
        return null;
    }


}
