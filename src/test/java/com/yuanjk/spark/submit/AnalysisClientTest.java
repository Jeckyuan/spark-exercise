package com.yuanjk.spark.submit;

import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yuanjk
 * @version 22/2/16
 */
public class AnalysisClientTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void openSession() {

        String conf = "akka {\n" +
                "  actor {\n" +
                "    provider = cluster\n" +
                "    allow-java-serialization = on\n" +
                "  }\n" +
                "  serializers {\n" +
                "    kryo = \"com.twitter.chill.akka.AkkaSerializer\"\n" +
                "  }\n" +
                "  serialization-bindings {\n" +
                "    \"java.io.Serializable\" = none\n" +
                "    \"scala.Product\" = kryo\n" +
                "  }\n" +
                "  remote {\n" +
                "    log-remote-lifecycle-events = off\n" +
                "    netty.tcp {\n" +
                "      hostname = \"" + "127.0.0.1" + "\"\n" +
                "      port = " + "43500" + "\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Config config = ConfigFactory.parseString(conf);


        System.out.println("configuration: \n" + (new Gson()).toJson(config));

        config = ConfigFactory.load();

//        System.out.println("all stack configuration: \n" + (new Gson()).toJson(config));


        System.out.println(conf);
    }

    @Test
    public void closeSession() {
    }

    @Test
    public void sendAnalyseRequest() {
    }
}