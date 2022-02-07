package com.yuanjk.spark.submit;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class MyActor extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class,
                        s -> {
                            log.info("Received String message: {}", s);
                            System.out.println("===receive message [" + s + "]===");
                            getSender().tell("===MESSAGE IS RECEIVED===", getSelf());
                        })
                .matchAny(o -> {
                    log.info("received unknown message");
                    System.out.println("===receive unknown message===");
                })
                .build();
    }
}
