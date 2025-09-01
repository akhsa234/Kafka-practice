package com.bahar.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class wikimediaChangeHandler implements EventHandler {

    private final Logger log = LoggerFactory.getLogger(wikimediaChangeHandler.class.getSimpleName());
    KafkaProducer<String, String> kafkaProducer;
    String topic;

    public wikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }


    @Override
    public void onOpen() throws Exception {
        //nothing
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent)  {
        log.info(messageEvent.getData());
        //async
        kafkaProducer.send( new ProducerRecord<>( topic,messageEvent.getData()));
            }

    @Override
    public void onComment(String comment)  {
        //nothing

    }

    @Override
    public void onError(Throwable t) {
        log.info("error in stream reading" , t);

    }
}
