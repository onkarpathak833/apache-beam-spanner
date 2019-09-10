package com.example.beam;

import com.google.cloud.spanner.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

import static com.example.beam.Constants.*;
import static com.example.beam.Constants.BATCH_SIZE;

class DataAccessor implements Serializable {

     PCollection<String> loadDataFromFileSystem(Pipeline pipeline, String location) {
         return pipeline.apply(TextIO.read().from(location)).setCoder(StringUtf8Coder.of());
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
