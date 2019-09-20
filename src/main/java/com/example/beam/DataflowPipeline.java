package com.example.beam;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.stream.Collectors;

import static com.example.beam.Constants.*;

public class DataflowPipeline {
    public static final String localDataLoaction = "C:\\Data";
    private static final String gcsDepartmentDataLocation = "gs://beam-datasets-tw/Department.csv";
    private static final String gcsEmployeeDataLocation = "gs://beam-datasets-tw/Employee.csv";
    private static final DataAccessor dao = new DataAccessor();
    private static final SpannerBusinessLayer businessLayer = new SpannerBusinessLayer();
    private static List<String> tableNames = null;
    private static JSONObject keysObject = new JSONObject();

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
                    Set<String> tableKeys = keysObject.keySet();
                    Map keyValue = new HashMap<String, Object>();
                    tableKeys.stream().map(key -> keyValue.put(key, keysObject.get(key))).collect(Collectors.toList());

                    String tableSchema = dao.getTableAvroSchema(table);
                    System.out.println("Table Schema : " + tableSchema);
                    dao.loadDataFromJdbc(pipeline, table, keyValue, tableSchema)
                            .apply(ParDo.of(new DoFn<Integer, String>() {
                                @ProcessElement
                                public void processElement(ProcessContext context) {
                                    System.out.println("Record :  : " + context.element());
                                }
                            }));
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
