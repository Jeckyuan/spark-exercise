package com.yuanjk.spark.submit;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.yuanjk.spark.data.DataSourceIgnite;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MyActor extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class,
                        s -> {
                            log.info("Received String message: {}", s);
                            System.out.println("===receive message [" + s + "]===");

                            Dataset<Row> df = DataSourceIgnite.loadDataFromIgnite(s);

                            log.info("===dataframe count: {}===", df.count());

                            String sql = "SELECT * FROM " + s + " WHERE ST_Contains(ST_GeomFromWKT(\"" +
                                    "MultiPolygon (((-122.26027952007120803 47.6506526623547586, " +
                                    "-122.21306882130804183 47.65021754070716753, " +
                                    "-122.21654979448874201 47.61714829549057271, " +
                                    "-122.25723366853810603 47.62998438409438506, " +
                                    "-122.26027952007120803 47.6506526623547586)))" +
                                    "\")," + "ST_GeomFromWKT( CIM_GEOGRAPHICINFORMATION )" + ")";

                            System.out.println("===contain query sql [" + sql + "]===");
                            df = DataSourceIgnite.querySql(s, sql);

                            df.collectAsList().forEach(r -> System.out.println(r.mkString()));

                            getSender().tell("===MESSAGE IS RECEIVED===", getSelf());
                        })
                .matchAny(o -> {
                    log.info("received unknown message");
                    System.out.println("===receive unknown message===");
                })
                .build();
    }
}
