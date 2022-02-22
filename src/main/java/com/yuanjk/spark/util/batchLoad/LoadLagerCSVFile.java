package com.yuanjk.spark.util.batchLoad;

import com.yuanjk.spark.util.IgniteUtil;
import com.yuanjk.spark.util.LoadDataToIgnite;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author yuanjk
 * @version 22/2/17
 */
public class LoadLagerCSVFile {

    public static void main(String[] args) throws InterruptedException {
        String schemaName = "CIM_STUDY";
        String tableName = "yellow_tripdata_2015_11_v3";
        String filePath = "G:\\data\\geospatial_data\\taxi_data\\yellow_tripdata_2015-11.csv";

        String endFlag = "DONE";
//        LoadDataToIgnite.setIgniteConfig("10.0.168.45:10800", "10.0.168.19:10800", "10.0.168.52:10800");
        LoadDataToIgnite.setIgniteConfig("10.2.112.43:10800");
        ClientConfiguration igniteConfig = LoadDataToIgnite.igniteConfig;

        BlockingQueue<String> blockingDeque = new ArrayBlockingQueue<String>(100 * 10000);
        CSVReadTask readTask = new CSVReadTask(blockingDeque, filePath, endFlag, 200 * 10000);
        Thread readThread = new Thread(readTask);
        readThread.start();
        for (int i = 0; i < 10; i++) {
            // thin client insert
             new Thread(new ThinClientInsertTask(igniteConfig, schemaName, tableName, endFlag, blockingDeque)).start();
//            new Thread(new InsertTask(IgniteUtil.getIgniteConfiguration(true), schemaName, tableName, endFlag,
//                    blockingDeque)).start();
        }
        readThread.join();
    }

}


class CSVReadTask implements Runnable {
    private final BlockingQueue<String> queue;
    private final String filePath;
    private final String endFlag;
    private final long totalRows;

    public CSVReadTask(BlockingQueue<String> queue, String filePath, String endFlag, long totalRows) {
        this.queue = queue;
        this.filePath = filePath;
        this.endFlag = endFlag;
        this.totalRows = totalRows;
    }

    @Override
    public void run() {
        long startDate = System.currentTimeMillis();
        long lineNumber = 1;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine();
            while (line != null) {
                if (lineNumber > 1) {
                    queue.put(line + "," + lineNumber);
                }
                if (lineNumber == totalRows) {
                    break;
                }
                lineNumber++;
                line = br.readLine();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        try {
            queue.put(endFlag);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Read file used million seconds: " + (System.currentTimeMillis() - startDate));
    }
}

class ThinClientInsertTask implements Runnable {

    private final BlockingQueue<String> queue;
    private final String endFlag;
    private final ClientConfiguration igniteConfig;
    private final String schemaName;
    private final String tableName;

    public ThinClientInsertTask(ClientConfiguration igniteConfig, String schemaName, String tableName, String endFlag,
                                BlockingQueue<String> queue) {
        this.igniteConfig = igniteConfig;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.endFlag = endFlag;
        this.queue = queue;
    }

    @Override
    public void run() {
        long startDate = System.currentTimeMillis();
        try (IgniteClient client = Ignition.startClient(igniteConfig)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String line = queue.take();
            while (!line.equalsIgnoreCase(endFlag)) {
                String[] arr = line.split(",");
                if (arr.length == 20) {
//                    System.out.println("======" + line);
                    long lineNumber = Long.valueOf(arr[19]);
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
                }
                line = queue.take();
            }
            queue.put(line);
            System.out.println(
                    "I have finished: " + Thread.currentThread()
                            .getName() + ", used million seconds: " + (System.currentTimeMillis() - startDate));
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

class InsertTask implements Runnable {

    private final BlockingQueue<String> queue;
    private final String endFlag;
    private final IgniteConfiguration igniteConfig;
    private final String schemaName;
    private final String tableName;

    public InsertTask(IgniteConfiguration igniteConfig, String schemaName, String tableName, String endFlag,
                      BlockingQueue<String> queue) {
        this.igniteConfig = igniteConfig;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.endFlag = endFlag;
        this.queue = queue;
    }

    @Override
    public void run() {
        long startDate = System.currentTimeMillis();
        Ignite ignite = Ignition.start(igniteConfig);
        IgniteCache igniteCache = ignite.cache(tableName);
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String line = queue.take();
            while (!line.equalsIgnoreCase(endFlag)) {
                String[] arr = line.split(",");
                if (arr.length == 20) {
//                    System.out.println("======" + line);
                    long lineNumber = Long.valueOf(arr[19]);
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

                    LoadDataToIgnite.dataLoaderServer(igniteCache, schemaName, tableName, values);
                }
                line = queue.take();
            }
            queue.put(line);
            System.out.println(
                    "I have finished: " + Thread.currentThread()
                            .getName() + ", used million seconds: " + (System.currentTimeMillis() - startDate));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ignite.close();
        }


    }
}