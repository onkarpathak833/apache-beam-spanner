package com.tw.beam;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.*;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.io.FileInputStream;
import java.util.Random;

import static com.tw.beam.Constants.*;

public class DataflowPipeline {

    public static void main(String[] args) {


        boolean isLocalFileSystem = true;
        if (isLocalFileSystem) {
            loadGoogleCredentials();
        }

        Pipeline pipeLine = createPipeline();

        String localDataLoaction = "/Users/techops/Documents";
        String gcsDataLocation = "gs://beam-datasets-tw";

        PCollection<String> collection = loadDataFromFileSystem(pipeLine, gcsDataLocation);

        PCollection<String> output = filterStockDataAndWriteToGCS(gcsDataLocation, collection);

        createSpannerWritableRecords(output);


        pipeLine.
                run()
                .waitUntilFinish();

    }

    private static void createSpannerWritableRecords(PCollection<String> output) {
        PCollection<Mutation> outputToSpanner = output.apply("createMutations", ParDo.of(new CreateSpannerMutation()));

        outputToSpanner.apply("writeToSpanner",
                SpannerIO.write()
                        .withProjectId(PROJECT_ID)
                        .withInstanceId(SPANNER_INSTANCE_ID)
                        .withDatabaseId(SPANNER_DB_ID)
                        .withBatchSizeBytes(BATCH_SIZE));
    }

    private static PCollection<String> filterStockDataAndWriteToGCS(String gcsDataLocation, PCollection<String> collection) {
        PCollection<String> output = collection.apply("Get Specific StockIds", ParDo.of(new DataFilter()));
        output.apply("WriteToFile", TextIO.write().to(gcsDataLocation + "/output.csv"));
        return output;
    }

    private static Pipeline createPipeline() {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        pipelineOptions.setProject(PROJECT_ID);
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setJobName(JOB_NAME);
        pipelineOptions.setStagingLocation(GCS_LOCATION);
        pipelineOptions.setTempLocation(GCS_LOCATION);
        FileSystems.setDefaultPipelineOptions(pipelineOptions);
        pipelineOptions.setJobName(JOB_NAME);
        return Pipeline.create(pipelineOptions);
    }

    private static PCollection<String> loadDataFromFileSystem(Pipeline pipeline, String location) {
        PCollection<String> collection = pipeline.apply(TextIO.read().from(location + "/data.csv")).setCoder(StringUtf8Coder.of());
        return collection;
    }

    //TODO: This method is currently not required
    private static void createSpannerDatabaseClient() {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();

        DatabaseId dbId = DatabaseId.of(InstanceId.of(PROJECT_ID, SPANNER_INSTANCE_ID), SPANNER_DB_ID);

        DatabaseClient client = spanner.getDatabaseClient(dbId);
    }

    private static void loadGoogleCredentials() {
        File file = new File(GCP_API_KEY);
        try {
            FileInputStream inputStream = new FileInputStream(file);
            GoogleCredentials credentials = ServiceAccountCredentials.fromStream(inputStream).toBuilder().build();
            String projectId = ((ServiceAccountCredentials) credentials).getProjectId();
            System.out.println(projectId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    static class DataFilter extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext pc) {
            String line = pc.element();
            String rowData[] = line.split(",");
            if (rowData[0].equals("104")) {
                pc.output(line);
            }
        }
    }

    static class CreateSpannerMutation extends DoFn<String, Mutation> {

        @ProcessElement
        public void processElement(ProcessContext pc) {

            String line = pc.element();
            String data[] = line.split(",");
            System.out.println(data[0] + " : " + data[2]);
            Random random = new Random();
            long randomMillis = System.currentTimeMillis() + random.nextInt();

            Mutation mutation = Mutation.newInsertOrUpdateBuilder("stock_prices")
                    .set(COLUMN_ID).to(data[0])
                    .set(COLUMN_TIMESTAMP).to(randomMillis)
                    .set(COLUMN_PRICE).to(Double.valueOf(data[2]))
                    .build();
            System.out.println("Mutation object created for table : " + mutation.getTable());
            pc.output(mutation);

        }
    }


}
