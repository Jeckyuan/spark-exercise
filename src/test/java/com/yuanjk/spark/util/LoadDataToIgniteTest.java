package com.yuanjk.spark.util;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author yuanjk
 * @version 22/2/10
 */
public class LoadDataToIgniteTest {

    @Before
    public void setUp() throws Exception {
        LoadDataToIgnite.setIgniteConfig("10.0.168.45:10800");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void dataLoader() {
        String schemaName = "CIM_STUDY";
        String tableName = "test_create_table_220210";

        Map<String, Object> values = new HashMap<>();

        values.put("id", 1L);
        values.put("name", "张三");

        LoadDataToIgnite.dataLoader(schemaName, tableName, values);

    }

    @Test
    public void createTable() {
        String schemaName = "CIM_STUDY";
        String tableName = "test_create_table_220210";
        Map<String, IgniteUtil.DataTypeEnum> columns = new HashMap<>();
        columns.put("id", IgniteUtil.DataTypeEnum.BIGINT);
        columns.put("name", IgniteUtil.DataTypeEnum.VARCHAR);

        String primaryKey = "id";

        LoadDataToIgnite.createTable(schemaName, tableName, columns, primaryKey);
    }


    @Test
    public void csvTableCreate() {
        String schemaName = "CIM_STUDY";
        String tableName = "yellow_tripdata_2015_11_v2";
        Map<String, IgniteUtil.DataTypeEnum> columns = new HashMap<>();
        columns.put("pid", IgniteUtil.DataTypeEnum.BIGINT);
        columns.put("vendor_id", IgniteUtil.DataTypeEnum.TINYINT);
        columns.put("tpep_pickup_datetime", IgniteUtil.DataTypeEnum.TIMESTAMP);
        columns.put("tpep_dropoff_datetime", IgniteUtil.DataTypeEnum.TIMESTAMP);
        columns.put("passenger_count", IgniteUtil.DataTypeEnum.TINYINT);
        columns.put("trip_distance", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("pickup_longitude", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("pickup_latitude", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("ratecode_id", IgniteUtil.DataTypeEnum.TINYINT);
        columns.put("store_and_fwd_flag", IgniteUtil.DataTypeEnum.VARCHAR);
        columns.put("dropoff_longitude", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("dropoff_latitude", IgniteUtil.DataTypeEnum.DOUBLE);

        columns.put("payment_type", IgniteUtil.DataTypeEnum.TINYINT);
        columns.put("fare_amount", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("extra", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("mta_tax", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("tip_amount", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("tolls_amount", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("improvement_surcharge", IgniteUtil.DataTypeEnum.DOUBLE);
        columns.put("total_amount", IgniteUtil.DataTypeEnum.DOUBLE);

        columns.put("CIM_GEOGRAPHICINFORMATION", IgniteUtil.DataTypeEnum.VARCHAR);

        String primaryKey = "pid";

        LoadDataToIgnite.createTable(schemaName, tableName, columns, primaryKey);

    }


    @Test
    public void csvDataLoader() throws IOException, ParseException {
        String schemaName = "CIM_STUDY";
        String tableName = "yellow_tripdata_2015_12";
        String filePath = "G:\\data\\geospatial_data\\taxi_data\\yellow_tripdata_2015-12.csv";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        BufferedReader br = new BufferedReader(new FileReader(filePath));

        Long startDate = System.currentTimeMillis();

        String line = br.readLine();
        Long lineNumber = 1L;
        while (line != null) {
            if (lineNumber == 1) {
                lineNumber++;
                line = br.readLine();
                continue;
            }
            String[] arr = line.split(",");
            if (arr.length == 19) {
                Map<String, Object> values = new HashMap<>();
                values.put("uuid", UUID.randomUUID());

                values.put("vendor_id", Byte.valueOf(arr[0]));
                values.put("tpep_pickup_datetime", sdf.parse(arr[1]));
                values.put("tpep_dropoff_datetime", sdf.parse(arr[2]));
                values.put("passenger_count", Byte.valueOf(arr[3]));
                values.put("trip_distance", Double.valueOf(arr[4]));

                values.put("pickup_longitude", Double.valueOf(arr[5]));
                values.put("pickup_latitude", Double.valueOf(arr[6]));
                values.put("ratecode_id", Byte.valueOf(arr[7]));
                values.put("store_and_fwd_flag", arr[8]);
                values.put("dropoff_longitude", Double.valueOf(arr[9]));
                values.put("dropoff_latitude", Double.valueOf(arr[10]));

                values.put("payment_type", Byte.valueOf(arr[11]));
                values.put("fare_amount", Double.valueOf(arr[12]));
                values.put("extra", Double.valueOf(arr[13]));
                values.put("mta_tax", Double.valueOf(arr[14]));
                values.put("tip_amount", Double.valueOf(arr[15]));

                values.put("tolls_amount", Double.valueOf(arr[16]));
                values.put("improvement_surcharge", Double.valueOf(arr[17]));
                values.put("total_amount", Double.valueOf(arr[18]));
                values.put("CIM_GEOGRAPHICINFORMATION", "POINT( " + arr[5] + " " + arr[6] + ")");

                LoadDataToIgnite.dataLoader(schemaName, tableName, values);
            } else {
                System.out.println("[" + line + "]");
            }
            if (lineNumber % 100 == 1) {
                System.out.println("loaded record number [" + lineNumber + "]");
            }
            line = br.readLine();
            lineNumber++;
        }
        System.out.println("used million seconds [" + (System.currentTimeMillis() - startDate) + "]");
    }

    @Test
    public void csvDataLoaderBatch() throws Exception {
        String schemaName = "CIM_STUDY";
        String tableName = "yellow_tripdata_2015_11";
        String filePath = "G:\\data\\geospatial_data\\taxi_data\\yellow_tripdata_2015-11.csv";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        BufferedReader br = new BufferedReader(new FileReader(filePath));

        Long startDate = System.currentTimeMillis();

        try (IgniteClient client = Ignition.startClient(LoadDataToIgnite.igniteConfig)) {


            String line = br.readLine();
            Long lineNumber = 1L;

            long roundDate = System.currentTimeMillis();
            while (line != null) {
                if (lineNumber == 1) {
                    lineNumber++;
                    line = br.readLine();
                    continue;
                }
                String[] arr = line.split(",");
                if (arr.length == 19) {
                    Map<String, Object> values = new HashMap<>();
                    values.put("uuid", UUID.randomUUID());

                    values.put("vendor_id", Byte.valueOf(arr[0]));
                    values.put("tpep_pickup_datetime", sdf.parse(arr[1]));
                    values.put("tpep_dropoff_datetime", sdf.parse(arr[2]));
                    values.put("passenger_count", Byte.valueOf(arr[3]));
                    values.put("trip_distance", Double.valueOf(arr[4]));

                    values.put("pickup_longitude", Double.valueOf(arr[5]));
                    values.put("pickup_latitude", Double.valueOf(arr[6]));
                    values.put("ratecode_id", Byte.valueOf(arr[7]));
                    values.put("store_and_fwd_flag", arr[8]);
                    values.put("dropoff_longitude", Double.valueOf(arr[9]));
                    values.put("dropoff_latitude", Double.valueOf(arr[10]));

                    values.put("payment_type", Byte.valueOf(arr[11]));
                    values.put("fare_amount", Double.valueOf(arr[12]));
                    values.put("extra", Double.valueOf(arr[13]));
                    values.put("mta_tax", Double.valueOf(arr[14]));
                    values.put("tip_amount", Double.valueOf(arr[15]));

                    values.put("tolls_amount", Double.valueOf(arr[16]));
                    values.put("improvement_surcharge", Double.valueOf(arr[17]));
                    values.put("total_amount", Double.valueOf(arr[18]));
                    values.put("CIM_GEOGRAPHICINFORMATION", "POINT( " + arr[5] + " " + arr[6] + ")");

                    LoadDataToIgnite.dataLoader(client, schemaName, tableName, values);
                } else {
                    System.out.println("[" + line + "]");
                }
                if (lineNumber % 10000 == 1) {
                    long tmp = System.currentTimeMillis();
                    System.out.println(
                            "loaded record number [" + lineNumber + "], used million seconds [" + (tmp - roundDate) + "]");
                    roundDate = tmp;
                }
                line = br.readLine();
                lineNumber++;
            }
        }

        System.out.println("used million seconds [" + (System.currentTimeMillis() - startDate) + "]");
    }

    @Test
    public void csvDataLoaderBatchV2() throws Exception {
        String schemaName = "CIM_STUDY";
        String tableName = "yellow_tripdata_2015_11_v2";
        String filePath = "G:\\data\\geospatial_data\\taxi_data\\yellow_tripdata_2015-11.csv";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        BufferedReader br = new BufferedReader(new FileReader(filePath));

        Long startDate = System.currentTimeMillis();

        try (IgniteClient client = Ignition.startClient(LoadDataToIgnite.igniteConfig)) {


            String line = br.readLine();
            Long lineNumber = 1L;

            long roundDate = System.currentTimeMillis();
            while (line != null) {
                if (lineNumber == 1) {
                    lineNumber++;
                    line = br.readLine();
                    continue;
                }
                String[] arr = line.split(",");
                if (arr.length == 19) {
                    Map<String, Object> values = new HashMap<>();
                    values.put("pid", lineNumber);

                    values.put("vendor_id", Byte.valueOf(arr[0]));
                    values.put("tpep_pickup_datetime", sdf.parse(arr[1]));
                    values.put("tpep_dropoff_datetime", sdf.parse(arr[2]));
                    values.put("passenger_count", Byte.valueOf(arr[3]));
                    values.put("trip_distance", Double.valueOf(arr[4]));

                    values.put("pickup_longitude", Double.valueOf(arr[5]));
                    values.put("pickup_latitude", Double.valueOf(arr[6]));
                    values.put("ratecode_id", Byte.valueOf(arr[7]));
                    values.put("store_and_fwd_flag", arr[8]);
                    values.put("dropoff_longitude", Double.valueOf(arr[9]));
                    values.put("dropoff_latitude", Double.valueOf(arr[10]));

                    values.put("payment_type", Byte.valueOf(arr[11]));
                    values.put("fare_amount", Double.valueOf(arr[12]));
                    values.put("extra", Double.valueOf(arr[13]));
                    values.put("mta_tax", Double.valueOf(arr[14]));
                    values.put("tip_amount", Double.valueOf(arr[15]));

                    values.put("tolls_amount", Double.valueOf(arr[16]));
                    values.put("improvement_surcharge", Double.valueOf(arr[17]));
                    values.put("total_amount", Double.valueOf(arr[18]));
                    values.put("CIM_GEOGRAPHICINFORMATION", "POINT( " + arr[5] + " " + arr[6] + ")");

                    LoadDataToIgnite.dataLoader(client, schemaName, tableName, values);
                } else {
                    System.out.println("[" + line + "]");
                }
                if (lineNumber % 10000 == 1) {
                    long tmp = System.currentTimeMillis();
                    System.out.println(
                            "loaded record number [" + lineNumber + "], used million seconds [" + (tmp - roundDate) + "]");
                    roundDate = tmp;
                }
                line = br.readLine();
                lineNumber++;
            }
        }

        System.out.println("used million seconds [" + (System.currentTimeMillis() - startDate) + "]");
    }

    @Test
    public void emptyTable() {
        String tableName = "yellow_tripdata_2015_11";

        LoadDataToIgnite.emptyTable(tableName);
    }


    @Test
    public void isCacheExists() {
    }

    @Test
    public void setIgniteConfig() {
    }
}