package com.yuanjk.spark.submit;

import com.yuanjk.spark.data.DataSourceIgnite;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator;
import org.apache.spark.serializer.KryoSerializer;
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
                .config("spark.serializer", KryoSerializer.class.getName())
                .config("spark.kryo.registrator", SedonaVizKryoRegistrator.class.getName())
                .getOrCreate();

        SedonaSQLRegistrator.registerAll(spark);

        System.out.println("===result[" + spark.range(1, 2000).count() + "]");
        System.out.println("===complete to run===");

        DataSourceIgnite.setIgniteServerHost("10.0.168.52");
        DataSourceIgnite.setSparkSession(spark);
        DataSourceIgnite.setSchemaName("CIM_STUDY");

        System.out.println("===start server===");
        AnalysisServer server = new AnalysisServer("10.0.168.19", 8808);
//        AnalysisServer server = new AnalysisServer("127.0.0.1", 8808);
        server.openSession();
//        System.out.println("===dynamic result[" + spark.range(1, 1000000).count() + "]");
        System.out.println("===complete to server===");
    }

}
