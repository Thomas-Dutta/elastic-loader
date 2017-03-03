package com.phynum.elasticsearch;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by thomasdutta on 3/1/17.
 */
public class ResultSetConverter {
    ResultSet resultSet;

    ResultSetConverter() {

    }

    ResultSetConverter(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public JSONArray convert() throws SQLException{
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

//        for(int i=1; i<=resultSetMetaData.getColumnCount(); i++) {
////            columnMap.put(resultSetMetaData.getColumnName(i),resultSetMetaData.getColumnClassName(i));
//            System.out.println(resultSetMetaData.getColumnName(i) + ", " + resultSetMetaData.getColumnTypeName(i)
//                    + ", " + resultSetMetaData.getColumnType(i));
//        }

        JSONArray jsonArray =  new JSONArray();

        while(resultSet.next()) {
            JSONObject jsonObject = new JSONObject();

            for(int i=1; i<=resultSetMetaData.getColumnCount(); i++) {
                int columnType = resultSetMetaData.getColumnType(i);
                switch (columnType) {
                    case Types.ARRAY :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getArray(i));
                        break;

                    case Types.BIGINT :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getLong(i));
                        break;

                    case Types.BINARY :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getByte(i));
                        break;

                    case Types.BOOLEAN :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getBoolean(i));
                        break;

                    case Types.BLOB :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getBlob(i));
                        break;

                    case Types.DOUBLE :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getDouble(i));
                        break;

                    case Types.FLOAT :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getFloat(i));
                        break;

                    case Types.INTEGER :
                        jsonObject.put(resultSetMetaData.getColumnName(i), resultSet.getInt(i));
                        break;

                    case Types.VARCHAR :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getString(i));
                        break;

                    case Types.TINYINT :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getInt(i));
                        break;

                    case Types.SMALLINT :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getInt(i));
                        break;

                    case Types.DATE :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getDate(i));
                        break;

                    case Types.TIMESTAMP :
                        jsonObject.put(resultSetMetaData.getColumnName(i),resultSet.getTimestamp(i));
                        break;

                }
            }
            jsonArray.put(jsonObject);
        }


//        for(Map.Entry<String, String> entry : columnMap.entrySet()) {
//            jsonObject.put(entry.getKey(),entry.getValue());
//        }

        return jsonArray;
    }
}
