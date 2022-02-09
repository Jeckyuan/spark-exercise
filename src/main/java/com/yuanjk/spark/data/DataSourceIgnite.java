package com.yuanjk.spark.data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author yuanjk
 * @version 22/2/9
 */
public class DataSourceIgnite {

    private static String igniteServerHost;
    private static SparkSession sparkSession;
    private static String schemaName;

    public static Dataset<Row> loadDataFromIgnite(String tableName) {

        String jdbcUrl = "jdbc:ignite:thin://" + igniteServerHost + "/" + schemaName + "?partitionAwareness=true";

        Dataset<Row> df = sparkSession.sqlContext().read().format("jdbc")
                .option("url", jdbcUrl)
                .option("driver", "org.apache.ignite.IgniteJdbcThinDriver")
                .option("dbtable", tableName)
                .option("fetchSize", 10000)
                .load();
        df.createOrReplaceTempView(tableName);
        return df;
    }


    public static Dataset<Row> querySql(String dataFrame, String sql) {
        if (!sparkSession.catalog().tableExists(dataFrame)) {
            loadDataFromIgnite(dataFrame);
        }

        return sparkSession.sql(sql);
    }


    public static void setIgniteServerHost(String igniteServerHost) {
        DataSourceIgnite.igniteServerHost = igniteServerHost;
    }

    public static void setSparkSession(SparkSession sparkSession) {
        DataSourceIgnite.sparkSession = sparkSession;
    }

    public static void setSchemaName(String schemaName) {
        DataSourceIgnite.schemaName = schemaName;
    }
}
