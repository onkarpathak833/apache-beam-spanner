package com.example.beam;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.*;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import static com.example.beam.Constants.*;

public class DataflowPipeline {
    public static final String localDataLoaction = "C:\\Data";
    public static final String gcsDataLocation = "gs://beam-datasets-tw";

    public static void main(String[] args) {
        GoogleCredentials credentials = CredentialsManager.loadGoogleCredentials(GCP_API_KEY);
        Pipeline pipeLine = createDataflowPipeline();

        DataAccessor dao = new DataAccessor();
        SpannerBusinessLayer businessLayer = new SpannerBusinessLayer();

        PCollection<String> collection = dao.loadDataFromFileSystem(pipeLine, gcsDataLocation);
        PCollection<String> output = businessLayer.filterSpannerData(collection);
//        dao.writeToGCS(output, gcsDataLocation + "/output.csv");

        PCollection<Mutation> spannerMutations = businessLayer.createSpannerMutations(output);
        dao.writeSpannerMutations(spannerMutations);
        pipeLine.
                run()
                .waitUntilFinish();

    }


    private static Pipeline createDataflowPipeline() {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        pipelineOptions.setProject(PROJECT_ID);
        pipelineOptions.setRunner(DataflowRunner.class);
        FileSystems.setDefaultPipelineOptions(pipelineOptions);
        return Pipeline.create(pipelineOptions);
    }


}
