package com.bahar.kafka.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.beans.EventHandler;


public class wikimediaChangesHandler implements EventHandler {

    KafkaProducer<String,String> kafkaProducer;
    String topic;

    public wikimediaChangesHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    public void onOpen() throws java.lang.Exception{
    }

public void onClosed()
        throws java.lang.Exception{



}
public void onMessage(java.lang.String event,
                      MessageEvent messageEvent)
        throws java.lang.Exception{


}
public void onComment(java.lang.String comment)
        throws java.lang.Exception{

}

public void onError(java.lang.Throwable t){

}


}
