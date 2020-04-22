package com.shapira.examples.producer.avroclicks;

import JavaSessionize.avro.LogLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class AvroClicksProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        /*if (args.length != 2) {
            System.out.println("Please provide command line arguments: numEvents schemaRegistryUrl");
            System.exit(-1);
        }*/
        long events = Long.parseLong("100");
        String schemaUrl = "http://clm-pun-tsiuit:8081";

        Properties props = new Properties();
        // hardcoding the Kafka server URI for this example
        props.put("bootstrap.servers", "clm-pun-tsiuit:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        /*props.put("key.serializer", Serdes.String());
        props.put("value.serializer", Serdes.String());*/
        props.put("schema.registry.url", schemaUrl);
        // Hard coding topic too.
        String topic = "clicks";

        Producer<String, LogLine> producer = new KafkaProducer<String, LogLine>(props);

        Random rnd = new Random();
        for (long nEvents = 0; nEvents < events; nEvents++) {
            LogLine event = EventGenerator.getNext();

            // Using IP as key, so events from same IP will go to same partition
            ProducerRecord<String, LogLine> record = new ProducerRecord<String, LogLine>(topic, event.getIp().toString(), event);
            producer.send(record).get();
            System.out.println("Produced message: " + record.toString());

        }
        producer.flush();
        producer.close();

    }
}

