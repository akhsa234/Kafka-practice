package com.bahar.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.*;


import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class wikimediaChangesProducer {


    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // create producer peroperties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        //set producer properties
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentChanges";

        EventHandler eventHandler = new wikimediaChangeHandler(producer,topic);
        String url = "https://stream.wikimedia.org/v2/stream.recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // start the producer in another thread
            eventSource.start();
            // we produce for 10 minutes and block the program until then
        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}