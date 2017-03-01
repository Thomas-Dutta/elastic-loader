package com.phynum.elasticsearch;

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
            resultSet = statement.executeQuery("SELECT id, case_number, block FROM crime_details_test");


            while(resultSet.next()) {
                System.out.println(resultSet.getString(1) + "  " + resultSet.getString(2) + "  " + resultSet.getString(3));
            }
            connection.close();



        } catch (SQLException e) {
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

