package com.example.beam;

import com.google.cloud.spanner.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.example.beam.Constants.*;

class DataAccessor implements Serializable {
    public static String jsonSchema = null;
    private static Connection connection = null;
    private static JdbcIO.DataSourceConfiguration config = null;

    private static Logger logger = LoggerFactory.getLogger("SetupLogger");

    static {

        try {
            Class.forName("com.mysql.jdbc.Driver");
            logger.info("Database Driver loaded successfully");
        } catch (ClassNotFoundException e) {
            logger.error("Error in loading DB driver");
            e.printStackTrace();
        }
        try {
            connection = DriverManager.getConnection("jdbc:mysql://google/test?cloudSqlInstance=project1-186407:us-east1:example-mysql-db&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=1234&useUnicode=true&characterEncoding=UTF-8");
            logger.info("DB Connection Successful..!");
        } catch (SQLException e) {
            logger.error("Error in Database connection.");
            e.printStackTrace();
        }
        config = JdbcIO.DataSourceConfiguration
                .create("com.mysql.jdbc.Driver", "jdbc:mysql://google/test?cloudSqlInstance=project1-186407:us-east1:example-mysql-db&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=1234&useUnicode=true&characterEncoding=UTF-8");
    }

    static DatabaseClient getSpannerDatabaseClient(String projectID, String instanceID, String databaseID) {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();
        DatabaseId dbId = DatabaseId.of(InstanceId.of(projectID, instanceID), databaseID);
        return spanner.getDatabaseClient(dbId);
    }

    PCollection<String> loadDataFromFileSystem(Pipeline pipeline, String location) {
        return pipeline.apply(TextIO.read().from(location)).setCoder(StringUtf8Coder.of());
    }

    String getTableAvroSchema(String tableName) throws SQLException {
        ResultSet dbResultSet = connection.prepareStatement("SELECT DISTINCT column_name, data_type \n" +
                "FROM information_schema.columns\n" +
                "WHERE table_name = " + "'" + tableName + "'").executeQuery();
        JSONObject schemaObject = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        logger.info("Got Database Table details from information_schema.columns");
        while (dbResultSet.next()) {
            String columnName = dbResultSet.getString(1);
            String columnDataType = dbResultSet.getString(2);
            schemaObject.put("type", "record");
            schemaObject.put("name", tableName);
            JSONArray array = new JSONArray();
            JSONObject object = null;
            switch (columnDataType) {
                case ("int"):
                case ("numeric"): {
                    object = new JSONObject();
                    object.putOnce("name", columnName);
                    array.put("int");
                    array.put("null");
                    object.putOnce("type", array);
                    object.putOnce("default", null);
                    jsonArray.put(object);
                    break;
                }

                case "double":
                case "float": {
                    object = new JSONObject();
                    object.putOnce("name", columnName);
                    array.put("double");
                    array.put("null");
                    object.putOnce("type", array);
                    object.putOnce("default", null);
                    jsonArray.put(object);
                    break;
                }
                case "varchar":
                case "varying char":
                default: {
                    object = new JSONObject();
                    object.putOnce("name", columnName);
                    array.put("string");
                    array.put("null");
                    object.putOnce("type", array);
                    object.putOnce("default", null);
                    jsonArray.put(object);
                    break;
                }

            }

        }

        schemaObject.putOnce("fields", jsonArray);
        StringBuilder stb = new StringBuilder();
        String schemaString = stb.append(schemaObject.toString(5)).toString();
        logger.info("Successfully built Table Avro Schema");
        logger.info("Avro Schema : {}", schemaString);
        return schemaString;
    }

    PCollection<GenericRecord> loadDataFromJdbc(Pipeline pipeline, String queryString, String tableSchema) {
        PCollection<GenericRecord> noOfRecordsCollection = pipeline.apply(JdbcIO.<GenericRecord>read()
                .withDataSourceConfiguration(config)
                .withCoder(AvroCoder.of(new Schema.Parser().parse(tableSchema)))
                .withQuery(queryString)
                .withRowMapper((JdbcIO.RowMapper<GenericRecord>) resultSet -> {
                    logger.info("Reading table using query in JdbcIO");
                    return createGenericRecords(tableSchema, resultSet);
                }));
        return noOfRecordsCollection;
    }

    private GenericRecord createGenericRecords(String tableSchema, ResultSet resultSet) throws SQLException {
        Schema schema = new Schema.Parser().parse(tableSchema);
        List<Schema.Field> fields = schema.getFields();
        StringBuilder stb = new StringBuilder();
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : fields) {
            String fieldName = field.name();
            Object value = null;
            try {
                value = resultSet.getObject(fieldName);
                record.put(fieldName, value);
            } catch (Exception e) {
                logger.info("setting default value for column {} as its not in the resultset.", fieldName);
            }


        }
        logger.info("Successfully built Avro Generic Record");
        return record;
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

}
