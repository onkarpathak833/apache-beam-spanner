package com.example.beam;

import com.google.cloud.spanner.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.example.beam.Constants.*;

public class SpannerBusinessLayer {

    public PCollection<Mutation> createSpannerMutations(PCollection<String> collection) {
        return collection.apply("createMutations", ParDo.of(new CreateSpannerMutationsForDepartmentData()));
    }


    static class ProcessReadWriteTransaction extends DoFn<String, Void> {
        DatabaseClient dbClient = DataAccessor.getSpannerDatabaseClient(PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DB_ID);
        @ProcessElement
        public void processElement(ProcessContext context) {
            String line = context.element();
            context.sideInput();
            dbClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
                @Nullable
                @Override
                public Void run(TransactionContext transaction) throws Exception {
                    String[] deptData = line.split(",");

                    Mutation deptMutation = Mutation.newInsertOrUpdateBuilder(SPANNER_DEPARTMENT_TABLE_NAME)
                            .set(DEPT_ID).to(deptData[0])
                            .set("commit_time").to(Value.COMMIT_TIMESTAMP)
                            .set(DEPT_NAME).to(deptData[1])
                            .set(LOCATION).to(deptData[2])
                            .build();
                    transaction.buffer(deptMutation);
                    Struct row = transaction.readRow(SPANNER_DEPARTMENT_TABLE_NAME, Key.of(deptData[0]), Arrays.asList("dept_id"));
                    assert row != null;
                    String actualDeptId = row.getString("dept_id");
                    if (actualDeptId.equals(deptData[0])) {

                        PCollection<String> employeeData = employeeCollection.apply(Filter.by((SerializableFunction<String, Boolean>) input -> {
                            String employeeDeptId = input.split(",")[3];
                            return employeeDeptId.equals(deptData[0]);
                        }));

                        PCollection<Mutation> empMutations = employeeData.apply(MapElements.via(new SimpleFunction<String, Mutation>() {
                            @Override
                            public Mutation apply(String line) {
                                String[] empData = line.split(",");
                                Mutation mutation = Mutation.newInsertOrUpdateBuilder(SPANNER_EMPLOYEE_TABLE_NAME)
                                        .set(EMP_NAME).to(Integer.parseInt(empData[0]))
                                        .set(EMP_NAME).to(empData[1])
                                        .set(SALARY).to(Integer.parseInt(empData[2]))
                                        .set(DEPT_ID).to(empData[3])
                                        .set(AGE).to(Integer.parseInt(empData[4]))
                                        .build();
                                transaction.buffer(mutation);
                                return mutation;
                            }
                        }));

                    }
                    return null;
                }
            });
            return;
        }
    }



    static void executeReadWriteTransactionWith(PCollection<String> deptCollection, PCollection<String> employeeCollection) {
        final PCollectionView<String> employeeCollectionView = employeeCollection.apply(View.<String>asSingleton());
        deptCollection.apply(ParDo.of(new ProcessReadWriteTransaction()).withSideInputs(employeeCollectionView));
    }


    static class CreateSpannerMutationsForDepartmentData extends DoFn<String, Mutation> {
        @ProcessElement
        public void processElement(ProcessContext pc) {
            String line = pc.element();
            String[] data = line.split(",");
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
            String[] rowData = line.split(",");
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
