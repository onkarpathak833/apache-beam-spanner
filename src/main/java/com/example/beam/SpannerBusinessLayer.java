package com.example.beam;

import com.google.cloud.spanner.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.example.beam.Constants.*;

public class SpannerBusinessLayer implements Serializable {

    private static final String gcsEmployeeDataLocation = "gs://beam-datasets-tw/Employee.csv";
    private static final DatabaseClient dbClient = getDbClient();

    static DatabaseClient getDbClient() {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();
        DatabaseId dbId = DatabaseId.of(InstanceId.of(PROJECT_ID, SPANNER_INSTANCE_ID), SPANNER_DB_ID);
        return spanner.getDatabaseClient(dbId);
    }

    public PCollection<Mutation> createSpannerMutations(PCollection<String> collection) {
        return collection.apply("createMutations", ParDo.of(new CreateSpannerMutationsForDepartmentData()));
    }


    static void executeReadWriteTransactionWith(PCollection<String> deptCollection, PCollection<String> employeeCollection) {
        PCollectionView<List<String>> collection = employeeCollection.apply(View.asList());
        deptCollection.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                String line = context.element();
                String departmentId = line.split(",")[0];
                List<String> employeeList = context.sideInput(collection);


                String[] deptData = line.split(",");
                Mutation deptMutation = Mutation.newInsertOrUpdateBuilder(SPANNER_DEPARTMENT_TABLE_NAME)
                        .set(DEPT_ID).to(deptData[0])
                        .set("commit_time").to(Value.COMMIT_TIMESTAMP)
                        .set(DEPT_NAME).to(deptData[1])
                        .set(LOCATION).to(deptData[2])
                        .build();

                dbClient.write(Arrays.asList(deptMutation));

                dbClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
                    @Nullable
                    @Override
                    public Void run(TransactionContext transaction) throws Exception {
                        String[] deptData = line.split(",");
                        Struct row = transaction.readRow(SPANNER_DEPARTMENT_TABLE_NAME, Key.of(deptData[0]), Arrays.asList("dept_id", "dept_name", "location"));

                        if (row != null) {
                            System.out.println("Dept. Row is valid for key : " + deptData[0]);
                            String actualDeptId = row.getString("dept_id");
                            if (actualDeptId.equals(deptData[0])) {

                                List<String> valid = employeeList.stream().filter(employee -> {
                                    String employeeDeptID = employee.split(",")[3];
                                    return employeeDeptID.equals(deptData[0]);
                                }).collect(Collectors.toList());

                                List<Mutation> employeeMutations = valid.stream().map(employeeLine -> {
                                    String[] empData = employeeLine.split(",");
                                    System.out.println("Employee Data is : " + Arrays.asList(empData));
                                    return Mutation.newInsertOrUpdateBuilder(SPANNER_EMPLOYEE_TABLE_NAME)
                                            .set(EMP_ID).to(Integer.valueOf(empData[0]))
                                            .set(EMP_NAME).to(empData[1])
                                            .set(SALARY).to(Integer.valueOf(empData[2]))
                                            .set(DEPT_ID).to(actualDeptId)
                                            .set(AGE).to(Integer.valueOf(empData[4]))
                                            .build();
                                }).collect(Collectors.toList());

                                transaction.buffer(employeeMutations);
                            }
                        }
                        return null;
                    }
                });
                context.output(departmentId);
            }


        }).withSideInputs(collection));
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

    private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
        @Override
        public List<T> createAccumulator() {
            return new ArrayList<T>();
        }

        @Override
        public List<T> addInput(List<T> accumulator, T input) {
            accumulator.add(input);
            return accumulator;
        }

        @Override
        public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
            List<T> result = createAccumulator();
            for (List<T> accumulator : accumulators) {
                result.addAll(accumulator);
            }
            return result;
        }

        @Override
        public List<T> extractOutput(List<T> accumulator) {
            return accumulator;
        }

        @Override
        public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
            return ListCoder.of(inputCoder);
        }

        @Override
        public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
            return ListCoder.of(inputCoder);
        }
    }


}
