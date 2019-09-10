package com.example.beam;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import static com.example.beam.Constants.*;

public class SpannerBusinessLayer {

    public PCollection<Mutation> createSpannerMutations(PCollection<String> collection) {
        PCollection<Mutation> spannerMutations = collection.apply("createMutations", ParDo.of( new CreateSpannerMutation()));
        return spannerMutations;
    }


    static class CreateSpannerMutation extends DoFn<String, Mutation> {
        @ProcessElement
        public void processElement(ProcessContext pc) {
            String line = pc.element();
            String data[] = line.split(",");
            System.out.println(data[0] + " : " + data[2]);

            Mutation mutation = Mutation.newInsertOrUpdateBuilder(SPANNER_DEPARTMENT_TABLE_NAME)
                    .set(DEPT_ID).to(data[0])
                    .set(DEPT_NAME).to(data[1])
                    .set(LOCATION).to(data[2])
                    .set(COMMIT_TIMESTAMP).to(Value.COMMIT_TIMESTAMP)
                    .build();

            System.out.println("Mutation object created for table : " + mutation.getTable());
            pc.output(mutation);
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

    public PCollection<String> filterSpannerData(PCollection<String> collection) {
        PCollection<String> output = collection.apply("Get Specific StockIds", ParDo.of(new DataFilter()));
        return output;
    }
}
