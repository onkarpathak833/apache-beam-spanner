package com.example.beam;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.example.beam.Constants.GCP_API_KEY;
import static com.example.beam.Constants.PROJECT_ID;

public class DataflowPipeline {
    public static final String localDataLoaction = "C:\\Data";
    private static final String gcsDepartmentDataLocation = "gs://beam-datasets-tw/Department.csv";
    private static final String gcsEmployeeDataLocation = "gs://beam-datasets-tw/Employee.csv";
    private static final DataAccessor dao = new DataAccessor();
    private static final SpannerBusinessLayer businessLayer = new SpannerBusinessLayer();
    private static List<String> tableNames = null;
    private static JSONObject keysObject = new JSONObject();
    private static Logger logger = LoggerFactory.getLogger("MainLogger");

    static {
        StorageOptions options = StorageOptions.newBuilder().setProjectId(PROJECT_ID)
                .setCredentials(CredentialsManager.loadGoogleCredentials(GCP_API_KEY)).build();
        Storage storage = options.getService();
        Blob blob = storage.get("beam-datasets-tw", "jdbc.conf");
        String confJson = new String(blob.getContent());
        JSONObject confObject = new JSONObject(confJson);
        keysObject = confObject.getJSONObject("content").getJSONObject("keys");
        JSONArray targetObject = confObject.getJSONObject("content").getJSONArray("target");
        tableNames = new ArrayList<String>();
        targetObject.forEach(data -> tableNames.add(data.toString()));
    }


    public static void main(String[] args) {
        GoogleCredentials credentials = CredentialsManager.loadGoogleCredentials(GCP_API_KEY);
        Pipeline pipeline = createDataflowPipeline(args);
        runPipelineFromCloudSQL(pipeline);
//        runPipelineFromSpanner(pipeLine);
    }

    private static void runPipelineFromCloudSQL(Pipeline pipeline) {
        try {
            tableNames.forEach(table -> {
                try {
                    logger.info("Running pipeline for Table {} ", table);
                    Set<String> tableKeys = keysObject.keySet();
                    logger.info("Table Keys are {} ", tableKeys);
                    Map keyValue = new HashMap<String, Object>();
                    tableKeys.stream().map(key -> keyValue.put(key, keysObject.get(key))).collect(Collectors.toList());

                    logger.info("Table Query Parameters are : {}", keyValue);
                    String tableSchema = dao.getTableAvroSchema(table);
                    String tableQueryString = businessLayer.generateQueryString(keyValue, tableSchema, table);
                    dao.loadDataFromJdbc(pipeline, tableQueryString, tableSchema)
                            .apply(AvroIO.writeGenericRecords(tableSchema)
                                    .to("gs://beam-datasets-tw/df/")
                                    .withSuffix(".avro"));
                    logger.info("Uploaded Avro file to GCS");
                    pipeline.
                            run()
                            .waitUntilFinish();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });


        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static void runPipelineFromSpanner(Pipeline pipeline) {
        PCollection<String> departmentCollection = dao.loadDataFromFileSystem(pipeline, gcsDepartmentDataLocation);
        PCollection<String> employeeCollection = dao.loadDataFromFileSystem(pipeline, gcsEmployeeDataLocation);

        SpannerBusinessLayer.executeReadWriteTransactionWith(departmentCollection, employeeCollection);

        pipeline.
                run()
                .waitUntilFinish();
    }


    private static Pipeline createDataflowPipeline(String[] args) {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs(args)
                .create()
                .as(DataflowPipelineOptions.class);

        System.out.println(pipelineOptions.getProject());
        System.out.println(pipelineOptions.getStagingLocation());
        System.out.println(pipelineOptions.getGcpTempLocation());
        System.out.println(pipelineOptions.getRunner());
        System.out.println("Setting project ID . ");
        pipelineOptions.setProject(PROJECT_ID);
        return Pipeline.create(pipelineOptions);
    }


}
