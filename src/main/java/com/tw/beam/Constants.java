package com.tw.beam;

public class Constants {

    public static final String GCS_LOCATION = "gs://beam-datasets-tw/df/";
    public static final String PROJECT_ID = "project1-186407";
    public static final String JOB_NAME = "beam-spanner-test-id-104";
    public static final String SPANNER_INSTANCE_ID = "spanner-df-test";
    public static final String SPANNER_DB_ID = "stock_data";
    public static final String SPANNER_TABLE_NAME = "stock_prices";
    public static final String GCP_API_KEY = "/Users/techops/Documents/GCP_API_KEY2.json";
    public static final String COLUMN_ID = "id";
    public static final String COLUMN_TIMESTAMP = "timestring";
    public static final String COLUMN_PRICE = "price";

    public static final long BATCH_SIZE = 512;
}
