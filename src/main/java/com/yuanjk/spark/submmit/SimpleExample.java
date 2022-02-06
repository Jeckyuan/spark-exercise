package com.yuanjk.spark.submmit;

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
                .master("spark://10.0.168.19:7077")
                .getOrCreate();

        System.out.println("===result[" + spark.range(1, 2000).count() + "]");
        System.out.println("===complete to run===");
    }

}
