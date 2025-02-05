package com.bahar.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger logg= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());



    public static void main(String[] args) {
       logg.info("hello world ???");


        // create producer peroperties
        Properties properties =new Properties();
        
        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");//"key", "value"
        //connect to conduktor playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jass.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\\\"prasanna\\\" password=\\\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJwcmFzYW5uYSIsIm9yZ2FuaXphdGlvbklkIjo3MDQ2OSwidXNlcklkIjpudWxsLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJjYTE3OTkxYi1hNDNlLTQxNTktOTQyNS0yYjU3ZDhjMDZlNzEifX0.xUqONoItEtNqos1G6B0_yNoYKZin8vxhZ-ej-2RCdaI\\\";");
        properties.setProperty("sasl.mechanism", "PLIAN");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer=new KafkaProducer<>(properties);

        // create a Producer Record
        ProducerRecord<String, String> producerRecord=
                new ProducerRecord<>("demo_java", "Message 1");

        // send data
        producer.send(producerRecord);

        // tell the producer to send data and block until done -- synchronize
        producer.flush();
        //flush and close the producer
        producer.close();

    }
}
