package com.yuanjk.spark.submit;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.OnComplete;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AnalysisClient {

    private String hostName;
    private int hostPort;
    private ActorSystem actorSystem;
    private ActorRef localCommunicationActor;
    private ActorSelection remoteCommunicationActor;


    public static void main(String[] args) {
//        AnalysisClient client = new AnalysisClient("127.0.0.1", 9909);
//        AnalysisClient client = new AnalysisClient("10.0.168.52", 9909);
        AnalysisClient client = new AnalysisClient("10.2.112.68", 9909);

        client.openSession();

//        client.sendAnalyseRequest("MY TEST STRING", 10000);
        client.sendAnalyseRequest("TRAFFIC_CAMERAS", 10000);

    }

    public AnalysisClient(String hostName, int hostPort) {
        this.hostName = hostName;
        this.hostPort = hostPort;
    }

    public void openSession() {
        String configStr = "akka{" +
                "actor {" +
                "provider = cluster," +
                "allow-java-serialization = on" +
                "}," +
                "serializers {" +
                "kryo = \"com.twitter.chill.akka.AkkaSerializer\"" +
                "}," +
                "serialization-bindings {" +
                "java.io.Serializable = none," +
                "scala.Product = kryo" +
                "}," +
                "remote {" +
                "artery {" +
                "transport = tcp," +
                "canonical.hostname = \"" + this.hostName + "\"," +
                "canonical.port = " + this.hostPort +
                "}" +
                "}," +
                "loglevel=ERROR" +
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
//        System.out.println("===configuration===" + configStr);
        actorSystem = ActorSystem.create("EngineCommunicationTestSystem", config);
        localCommunicationActor = actorSystem.actorOf(Props.create(MyActor.class), "communicationRouter");

//        String engineCommunicationHostName = "127.0.0.1";
        String engineCommunicationHostName = "10.0.168.19";
        String engineCommunicationPort = "8808";
        String path = "akka.tcp://CIMAnalysisEngineCommunicationSystem@" + engineCommunicationHostName + ":" + engineCommunicationPort + "/user/communicationRouter";
        remoteCommunicationActor = actorSystem.actorSelection(path);
        System.out.println("===client remote actor path: " + remoteCommunicationActor.pathString());
    }

    public void closeSession() {
        if (actorSystem != null) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            actorSystem.terminate();
        } else {
//            throw new EngineClientInitException();
        }
    }

    public void sendAnalyseRequest(String analyseRequest, int timeoutSecond) {
        if (remoteCommunicationActor != null) {
            if (analyseRequest != null) {
//                analyseRequest.generateMetaInfo();
                Timeout timeout = new Timeout(Duration.create(timeoutSecond, TimeUnit.SECONDS));
                Future<Object> future = Patterns.ask(remoteCommunicationActor, analyseRequest, timeout);
                System.out.println("===request message is sent===");
                future.onComplete(new OnComplete<Object>() {
                    @Override
                    public void onComplete(Throwable throwable, Object o) throws Throwable {
                        if (throwable != null) {
                            System.out.println("返回结果异常：" + throwable.getMessage());
                            throwable.printStackTrace();
                        } else {
                            System.out.println("返回消息：" + o);
//                            analyseResponseCallback.onResponseReceived(o);
                        }
                    }
                }, actorSystem.dispatcher());
                // 成功，执行过程
                future.onSuccess(new OnSuccess<Object>() {
                    @Override
                    public void onSuccess(Object msg) throws Throwable {
                        System.out.println("回复的消息：" + msg);
//                        if (msg instanceof AnalyseResponse) {
//                            analyseResponseCallback.onSuccessResponseReceived((AnalyseResponse) msg);
//                        } else {
//                            analyseResponseCallback.onFailureResponseReceived(new AnalyseResponseFormatException());
//                        }
                    }
                }, actorSystem.dispatcher());
                //失败，执行过程
                future.onFailure(new OnFailure() {
                    @Override
                    public void onFailure(Throwable throwable) throws Throwable {
                        if (throwable instanceof TimeoutException) {
                            System.out.println("服务超时");
                            throwable.printStackTrace();
                        } else {
                            System.out.println("未知错误");
                        }
//                        analyseResponseCallback.onFailureResponseReceived(throwable);
                    }
                }, actorSystem.dispatcher());
            } else {
                System.out.println("analysis content is null");
            }
        } else {
            System.out.println("remote actor is null");
        }
    }

}
