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
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.example.beam.Constants.*;
import static com.example.beam.Constants.BATCH_SIZE;

class DataAccessor implements Serializable {
    private static Connection connection = null;
    public static String jsonSchema = null;
    private static JdbcIO.DataSourceConfiguration config = null;

    static {

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection = DriverManager.getConnection("jdbc:mysql://google/test?cloudSqlInstance=project1-186407:us-east1:example-mysql-db&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=1234&useUnicode=true&characterEncoding=UTF-8");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        jsonSchema = "{ \n" +
                "   \"name\":\"products\",\n" +
                "   \"type\":\"record\",\n" +
                "   \"fields\":[ \n" +
                "      { \n" +
                "         \"name\":\"product_id\",\n" +
                "         \"type\":[ \n" +
                "            \"int\",\n" +
                "            \"null\"\n" +
                "         ]\n" +
                "      },\n" +
                "      { \n" +
                "         \"name\":\"product_name\",\n" +
                "         \"type\":[ \n" +
                "            \"string\",\n" +
                "            \"null\"\n" +
                "         ]\n" +
                "      },\n" +
                "      { \n" +
                "         \"name\":\"product_desc\",\n" +
                "         \"type\":[ \n" +
                "            \"string\",\n" +
                "            \"null\"\n" +
                "         ]\n" +
                "      },\n" +
                "      { \n" +
                "         \"name\":\"category\",\n" +
                "         \"type\":[ \n" +
                "            \"string\",\n" +
                "            \"null\"\n" +
                "         ]\n" +
                "      },\n" +
                "      { \n" +
                "         \"name\":\"manufacturer\",\n" +
                "         \"type\":[ \n" +
                "            \"string\",\n" +
                "            \"null\"\n" +
                "         ]\n" +
                "      },\n" +
                "      { \n" +
                "         \"name\":\"base_price\",\n" +
                "         \"type\":[ \n" +
                "            \"double\",\n" +
                "            \"null\"\n" +
                "         ]\n" +
                "      },\n" +
                "      { \n" +
                "         \"name\":\"max_discount\",\n" +
                "         \"type\":[ \n" +
                "            \"double\",\n" +
                "            \"null\"\n" +
                "         ]\n" +
                "      },\n" +
                "      { \n" +
                "         \"name\":\"retail_price\",\n" +
                "         \"type\":[ \n" +
                "            \"double\",\n" +
                "            \"null\"\n" +
                "         ]\n" +
                "      }\n" +
                "   ]\n" +
                "}";
        config = JdbcIO.DataSourceConfiguration
                .create("com.mysql.jdbc.Driver", "jdbc:mysql://google/test?cloudSqlInstance=project1-186407:us-east1:example-mysql-db&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=1234&useUnicode=true&characterEncoding=UTF-8");
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
        int columns = dbResultSet.getMetaData().getColumnCount();
        while (dbResultSet.next()) {
            String columnName = dbResultSet.getString(1);
            String columnDataType = dbResultSet.getString(2);
            System.out.println(columnName);
            System.out.println(columnDataType);
            schemaObject.put("type", "record");
            schemaObject.put("name", tableName);
            JSONArray array = new JSONArray();
            JSONObject object = null;
            switch (columnDataType) {
                case "int": {
                    object = new JSONObject();
                    object.putOnce("name", columnName);
                    array.put("int");
                    array.put("null");
                    object.putOnce("type", array);
                    jsonArray.put(object);
                    break;
                }
                case "varchar": {
                    object = new JSONObject();
                    object.putOnce("name", columnName);
                    array.put("string");
                    array.put("null");
                    object.putOnce("type", array);
                    jsonArray.put(object);
                    break;
                }

                case "decimal": {
                    object = new JSONObject();
                    object.putOnce("name", columnName);
                    array.put("double");
                    array.put("null");
                    object.putOnce("type", array);
                    jsonArray.put(object);
                    break;
                }
                default: {
                    object = new JSONObject();
                    object.putOnce("name", columnName);
                    array.put("string");
                    array.put("null");
                    object.putOnce("type", array);
                    jsonArray.put(object);
                    break;
                }

            }

        }

        schemaObject.putOnce("fields", jsonArray);
        StringBuilder stb = new StringBuilder();
        String schema = stb.append(schemaObject.toString(5)).toString();
        return schema;
    }

    PCollection<String> loadDataFromJdbc(Pipeline pipeline, String tableName, Map<String, Object> whereParameters, String tableSchema) {
        AtomicReference<String> sql = new AtomicReference<>("SELECT * FROM " + tableName + " WHERE ");
        whereParameters.keySet().forEach(key -> {
            Object value = whereParameters.get(key);
            sql.set(sql + key + " = " + Integer.valueOf(value.toString()) + " AND ");
        });

        String tempString = sql.get().substring(0, sql.get().lastIndexOf(" AND "));
        String queryString = tempString.trim();
        System.out.println("Query String : " + queryString);
//        System.out.println(schemaString);

        PCollection<String> noOfRecordsCollection = pipeline.apply(JdbcIO.<String>read()
                .withDataSourceConfiguration(config)
                .withCoder(StringUtf8Coder.of())
                .withQuery(queryString)
                .withRowMapper((JdbcIO.RowMapper<String>) resultSet -> {
                    int noOfRecords = 0;
                    System.out.println("Schema here : " + tableSchema);
                    Schema schema = new Schema.Parser().parse(tableSchema);
                    List<Schema.Field> fields = schema.getFields();
                    StringBuilder stb = new StringBuilder();
                    noOfRecords = noOfRecords + 1;
                    System.out.println("No of records here: " + noOfRecords);
                    GenericRecord record = new GenericData.Record(schema);
                    for (Schema.Field field : fields) {
                        String fieldName = field.name();
                        Object value = resultSet.getObject(fieldName);
                        System.out.println("Field name : " + fieldName + " Value : " + value);
                        record.put(fieldName, value);
                    }
                    System.out.println("Generic Record : " + record);

                    stb.append(record.toString());
                    stb.append("\n");
                    return stb.toString();
                }));
        return noOfRecordsCollection;
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

    private static class StringCombiner<T> extends Combine.CombineFn<java.lang.String, List<java.lang.String>, java.lang.String> {

        @Override
        public List<java.lang.String> createAccumulator() {
            return new ArrayList<java.lang.String>();
        }

        @Override
        public List<java.lang.String> addInput(List<java.lang.String> mutableAccumulator, java.lang.String input) {
            mutableAccumulator.add(input);
            return mutableAccumulator;
        }

        @Override
        public List<java.lang.String> mergeAccumulators(Iterable<List<java.lang.String>> accumulators) {
            List<java.lang.String> result = createAccumulator();
            for (List<java.lang.String> accumulator : accumulators) {
                result.addAll(accumulator);
            }
            return result;
        }

        @Override
        public java.lang.String extractOutput(List<java.lang.String> accumulator) {
            java.lang.String result = "";
            for (java.lang.String data : accumulator) {
                result = result + data;
            }
            return result;
        }
    }
}
