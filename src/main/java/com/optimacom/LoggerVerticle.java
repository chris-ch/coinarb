package com.optimacom;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.eventbus.MessageConsumer;

public class LoggerVerticle extends AbstractVerticle {

    private String loggerName = null;
    private String channelName = null;

    public LoggerVerticle(String loggerName, String channelName){
        this.loggerName = loggerName;
        this.channelName = channelName;
    }

    public void start(Future<Void> startFuture) {
        Logger logger = LoggerFactory.getLogger("LoggerVerticle");
        vertx.eventBus().consumer(this.channelName, message -> {
            logger.info(this.loggerName + ":" + message.body());
        });
    }
}