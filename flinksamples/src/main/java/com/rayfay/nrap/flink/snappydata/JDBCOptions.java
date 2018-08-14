package com.rayfay.nrap.flink.snappydata;

import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.calcite.shaded.com.google.common.collect.Sets;

import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JDBCOptions implements Serializable {
    public static final Set<String> jdbcOptionNames = Sets.newHashSet();
    public static final String  JDBC_URL = newOption("url");
    public static final String  JDBC_TABLE_NAME = newOption("dbtable");
    public static final String  JDBC_DRIVER_CLASS = newOption("driver");
    public static final String  JDBC_PARTITION_COLUMN = newOption("partitionColumn");
    public static final String  JDBC_LOWER_BOUND = newOption("lowerBound");
    public static final String  JDBC_UPPER_BOUND = newOption("upperBound");
    public static final String  JDBC_NUM_PARTITIONS = newOption("numPartitions");
    public static final String  JDBC_BATCH_FETCH_SIZE = newOption("fetchsize");
    public static final String  JDBC_TRUNCATE = newOption("truncate");
    public static final String  JDBC_CREATE_TABLE_OPTIONS = newOption("createTableOptions");
    public static final String  JDBC_BATCH_INSERT_SIZE = newOption("batchsize");
    public static final String  JDBC_TXN_ISOLATION_LEVEL = newOption("isolationLevel");

    private Map<String, String> jdbcOptions = Maps.newHashMap();

    ///Default value
    public static final String defaultDriverClass = "io.snappydata.jdbc.ClientDriver";


    public JDBCOptions(Map<String, String> parameters){
        jdbcOptions.putAll(parameters);
    }
    public JDBCOptions(String url, Map<String, String> parameters){
        jdbcOptions.putAll(parameters);
        jdbcOptions.put(JDBC_URL, url);
        jdbcOptions.put(JDBC_DRIVER_CLASS, defaultDriverClass);
    }

    public String getJdbcUrl(){
        return jdbcOptions.get(JDBC_URL);
    }

    public String getJdbcDriverClass(){
        String driverClz = jdbcOptions.get(JDBC_DRIVER_CLASS);
        if(driverClz == null){
            try {
                driverClz = DriverManager.getDriver(getJdbcUrl()).getClass().getCanonicalName();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return driverClz;
    }

    public Properties asProperties(){
        Properties properties = new Properties();
        Iterator<Map.Entry<String, String>> entrys = jdbcOptions.entrySet().iterator();
        while (entrys.hasNext()){
            Map.Entry<String, String> en = entrys.next();
            properties.put(en.getKey(), en.getValue());
        }
        return properties;
    }

    @Override
    public String toString(){
        return getJdbcDriverClass()+":"+getJdbcUrl();
    }

    public Properties asConnectionProperties(){
        Properties properties = new Properties();
        Iterator<Map.Entry<String, String>> entrys = jdbcOptions.entrySet().iterator();
        while (entrys.hasNext()){
            Map.Entry<String, String> en = entrys.next();
            if(!jdbcOptionNames.contains(en.getKey())){

                properties.put(en.getKey(), en.getValue());
            }
        }
        return properties;
    }
    /**
     * private
     * @param name
     * @return
     */
    private static String newOption(String  name) {
        jdbcOptionNames.add(name.toLowerCase());
        return name;
    }
}
