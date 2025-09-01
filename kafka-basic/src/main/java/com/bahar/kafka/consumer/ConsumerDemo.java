package com.bahar.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger logg= LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());



    public static void main(String[] args) {
       logg.info("Consumer ???");

      String  groupId="java-app";
        String topic = "second_topic";

        // create Consumer peroperties
        Properties properties =new Properties();
        
        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");//"key", "value"

        //connect to conduktor playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jass.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\\\"prasanna\\\" password=\\\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJwcmFzYW5uYSIsIm9yZ2FuaXphdGlvbklkIjo3MDQ2OSwidXNlcklkIjpudWxsLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJjYTE3OTkxYi1hNDNlLTQxNTktOTQyNS0yYjU3ZDhjMDZlNzEifX0.xUqONoItEtNqos1G6B0_yNoYKZin8vxhZ-ej-2RCdaI\\\";");
        properties.setProperty("sasl.mechanism", "PLIAN");

        //set Consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);

        properties.setProperty("auto.offset.reset", "earliest");//"none/earliest/latest"

        // create the Consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data
        while (true){
            logg.info("polling");
            ConsumerRecords<String,String> records =
                            consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String,String> record: records){
                logg.info(" key : " + record.key() + "value : " +record.value());
                logg.info(" partition : " + record.partition() + "offset : " +record.offset());
            }
        }

    }
}
