package com.example.beam;

import com.google.cloud.spanner.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.sql.*;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.example.beam.Constants.*;
import static com.example.beam.Constants.BATCH_SIZE;

class DataAccessor implements Serializable {

    public static JSONObject schemaObject = new JSONObject();

    PCollection<String> loadDataFromFileSystem(Pipeline pipeline, String location) {
        return pipeline.apply(TextIO.read().from(location)).setCoder(StringUtf8Coder.of());
    }

    PCollection<String> loadDataFromJdbc(Pipeline pipeline, String tableName, Map<String, Object> whereParameters) throws ClassNotFoundException, SQLException {
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
                "WHERE table_name = " + "'" + tableName + "'");
        schemaObject.put("type", "record");
        schemaObject.put("namespace", "test");
        schemaObject.put("name", tableName);
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
        schemaObject.put("fields", jsonArray);
        System.out.println(schemaObject.toString());

        AtomicReference<String> sql = new AtomicReference<>("SELECT * FROM " +tableName  + " WHERE ");
        whereParameters.keySet().forEach(key -> {
            Object value = whereParameters.get(key);
            sql.set(sql  + key + " = " + Integer.valueOf(value.toString()) + " AND ");
        });

        String tempString = sql.get().substring(0, sql.get().lastIndexOf(" AND "));
        String queryString = tempString.trim();
        System.out.println("Query String : " + queryString);

        PCollection<String> jdbcCollection = (PCollection<String>) pipeline.apply(JdbcIO.<String>read().withDataSourceConfiguration(config)
                .withQuery(queryString)
                .withCoder(StringUtf8Coder.of())
                .withRowMapper((JdbcIO.RowMapper<String>) resultSet1 -> {
                    Schema schema = Schema.parse(schemaObject.toString(), true);
                    StringBuilder outputRecord = new StringBuilder();
                    System.out.println("Schema Object is : "+schema.toString());
                    while (resultSet1.next()) {
                        GenericRecord record = new GenericData.Record(schema);
                        List<Schema.Field> fieldsList = schema.getFields();
                        for (int i = 0; i < fieldsList.size(); i++) {
                            Schema.Field field = fieldsList.get(i);
//                            System.out.println(field.name());
                            Object value = resultSet1.getString(field.name());
                            record.put(field.name(), value);
                        }

                        System.out.println(" JDBC Record : " + record.toString());
                        outputRecord.append(record.toString());
                    }


                    return outputRecord.toString();
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
