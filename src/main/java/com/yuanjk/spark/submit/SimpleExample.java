package com.yuanjk.spark.submit;

import org.apache.spark.sql.SparkSession;

/**
 * @author yuanjk
 * @version 22/1/27
 */
public class SimpleExample {

    public static void main(String[] args) {
        System.out.println("===start to run===");
        SparkSession spark = SparkSession
                .builder()
//                .master("spark://10.0.168.19:7077")
//                .master("local[*]")
                .getOrCreate();

        System.out.println("===result[" + spark.range(1, 2000).count() + "]");
        System.out.println("===complete to run===");

        System.out.println("===start server===");
        AnalysisServer server = new AnalysisServer("10.0.168.19", 8808);
//        AnalysisServer server = new AnalysisServer("127.0.0.1", 8084);
        server.openSession();
//        System.out.println("===dynamic result[" + spark.range(1, 1000000).count() + "]");
        System.out.println("===complete to server===");
    }

}
