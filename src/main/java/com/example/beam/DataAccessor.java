package com.example.beam;

import com.google.cloud.spanner.*;
import netscape.javascript.JSObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.map.util.JSONPObject;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.*;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static com.example.beam.Constants.*;
import static com.example.beam.Constants.BATCH_SIZE;

class DataAccessor implements Serializable {

    PCollection<String> loadDataFromFileSystem(Pipeline pipeline, String location) {
        return pipeline.apply(TextIO.read().from(location)).setCoder(StringUtf8Coder.of());
    }

    PCollection<String> loadDataFromJdbc(Pipeline pipeline) throws ClassNotFoundException, SQLException {
        JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration
                .create("org.postgresql.Driver", "jdbc:postgresql://34.74.5.177/test")
                .withUsername("onkar")
                .withPassword("1234");

        Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection("jdbc:postgresql://34.74.5.177/test", "onkar", "1234");

        DatabaseMetaData dbMetadata = connection.getMetaData();
        ResultSet resultSet = dbMetadata.getTables(null, null, "product%", new String[]{"TABLE"});

        ResultSet rs = connection.createStatement().executeQuery("SELECT DISTINCT column_name, data_type \n" +
                "FROM information_schema.columns\n" +
                "WHERE table_name = 'products'");
        org.json.JSONObject jsonObject = new org.json.JSONObject();
        jsonObject.put("type", "record");
        jsonObject.put("namespace", "test");
        jsonObject.put("name", "products");
        JSONArray jsonArray = new JSONArray();

        while (rs.next()) {
            String columnName = rs.getString(1);
            String columnDataType = rs.getString(2);
            JSONObject object = new JSONObject();
            switch (columnDataType) {
                case "integer": {
                    object.put("name", columnName);
                    object.put("type", "int");
                    jsonArray.put(object);
                    break;
                }

                case "character varying": {
                    object.put("name", columnName);
                    object.put("type", "string");
                    jsonArray.put(object);
                    break;
                }

                case "numeric": {
                    object.put("name", columnName);
                    object.put("type", "double");
                    jsonArray.put(object);
                    break;
                }

                default: {
                    object.put("name", columnName);
                    object.put("type", "string");
                    jsonArray.put(object);
                    break;
                }
            }
        }
        jsonObject.put("fields", jsonArray);
        System.out.println(jsonObject.toString());
        Schema schema = Schema.parse(jsonObject.toString(), true);

        PCollection<String> jdbcCollection = (PCollection<String>) pipeline.apply(JdbcIO.<String>read().withDataSourceConfiguration(config)
                .withQuery("select * from products")
                .withCoder(StringUtf8Coder.of())
                .withRowMapper((JdbcIO.RowMapper<String>) resultSet1 -> {
                    GenericRecord record = new GenericData.Record(schema);
                    while (resultSet1.next()) {
                        List fieldsList = schema.getFields();
                        for (int i = 0; i < fieldsList.size(); i++) {
                            Object value = resultSet1.getString(fieldsList.get(i).toString());
                            record.put(fieldsList.get(i).toString(), value);
                        }
                    }

                    return record.toString();
                }));

        return jdbcCollection;
    }


    public void writeSpannerMutations(PCollection<Mutation> collection) {
        collection.apply("writeToSpanner",
                SpannerIO.write()
                        .withProjectId(PROJECT_ID)
                        .withInstanceId(SPANNER_INSTANCE_ID)
                        .withDatabaseId(SPANNER_DB_ID)
                        .withBatchSizeBytes(BATCH_SIZE));
    }

    public void writeToGCS(PCollection<String> collection, String gcsLocation) {
        collection.apply("WriteToFile", TextIO.write().to(gcsLocation));
    }

    static DatabaseClient getSpannerDatabaseClient(String projectID, String instanceID, String databaseID) {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();
        DatabaseId dbId = DatabaseId.of(InstanceId.of(projectID, instanceID), databaseID);
        return spanner.getDatabaseClient(dbId);
    }
}
