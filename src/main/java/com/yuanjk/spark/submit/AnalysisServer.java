package com.yuanjk.spark.submit;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AnalysisServer {

    private String hostName;
    private int hostPort;
    private ActorSystem actorSystem;
    private ActorRef localCommunicationActor;
    private ActorSelection remoteCommunicationActor;


    public static void main(String[] args) {
        AnalysisServer server = new AnalysisServer("127.0.0.1", 8808);
        server.openSession();
    }

    public AnalysisServer(String hostName, int hostPort) {
        this.hostName = hostName;
        this.hostPort = hostPort;
    }

    public void openSession() {
        String configStr = "akka {\n" +
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
                "    artery {\n" +
                "      transport = tcp # See Selecting a transport below\n" +
                "      canonical.hostname = " + this.hostName + "\n" +
                "      canonical.port = " + this.hostPort + "\n" +
                "      advanced {\n" +
                "          maximum-frame-size = 1 GiB\n" +
                "          buffer-pool-size = 512\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";


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
                "      hostname = \"" + this.hostName + "\"\n" +
                "      port = " + this.hostPort + "\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Config config = ConfigFactory.parseString(conf);
        actorSystem = ActorSystem.create("CIMAnalysisEngineCommunicationSystem", config);
        localCommunicationActor = actorSystem.actorOf(Props.create(MyActor.class), "communicationRouter");
        System.out.println("===server actor path: " + localCommunicationActor.path());
    }

}
