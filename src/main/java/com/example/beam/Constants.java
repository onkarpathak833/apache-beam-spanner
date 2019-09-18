package com.example.beam;

public class Constants {

    public static final String GCS_LOCATION = "gs://beam-datasets-tw/df";
    public static final String PROJECT_ID = "project1-186407";
    public static final String JOB_NAME = "beam-spanner-test-id-104";
    public static final String SPANNER_INSTANCE_ID = "spanner-example";
    public static final String SPANNER_DB_ID = "test";
    public static final String SPANNER_EMPLOYEE_TABLE_NAME = "employee";
    public static final String SPANNER_DEPARTMENT_TABLE_NAME = "department";
    public static final String GCP_API_KEY = "C:\\Data\\etl_sa_key.json";
    public static final String EMPLOYEE_DATA = "/Employee.csv";
    public static final String DEPT_DATA = "/Department.csv";
    public static final String EMP_ID = "emp_id";
    public static final String DEPT_ID = "dept_id";
    public static final String AGE = "age";
    public static final String EMP_NAME = "emp_name";
    public static final String SALARY = "salary";
    public static final String DEPT_NAME = "dept_name";
    public static final String LOCATION = "location";
    public static final String COLUMN_TIMESTAMP = "timestring";
    public static final String COLUMN_PRICE = "price";
    public static final String COMMIT_TIMESTAMP = "commit_time";
    public static final long BATCH_SIZE = 512;
}
