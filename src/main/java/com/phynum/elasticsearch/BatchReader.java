package com.phynum.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.UnknownHostException;
import java.sql.*;


/**
 * Created by thomasdutta on 3/1/17.
 */
public class BatchReader implements Runnable{

    public void run() {
        Connection connection = null;

        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/crime_data_set?" +
                                "user=root&password=rootuserpassword");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Statement statement  = null;
        ResultSet resultSet = null;


        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT * FROM crime_details where year = 2016 limit 10000");

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//            for(int i=1; i <= resultSetMetaData.getColumnCount() ; i++ ) {
//                System.out.println(resultSetMetaData.getColumnName(i));
//            }

//            while(resultSet.next()) {
//                System.out.println(resultSet.getString(1) + "  " + resultSet.getString(2) + "  " + resultSet.getString(3));
//            }


            ResultSetConverter resultSetConverter = new ResultSetConverter(resultSet);
            JSONArray jsonArray = resultSetConverter.convert();

//            for(int i=0; i<jsonArray.length(); i++) {
//                JSONObject jsonObject = jsonArray.getJSONObject(i);
//                System.out.println(jsonObject.toString(4));
//            }

            ElasticLoader elasticLoader = new ElasticLoader();
            elasticLoader.doThings(jsonArray);

            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } finally {
            if(resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            resultSet = null;

            if(statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                statement = null;
            }
        }

    }
}

