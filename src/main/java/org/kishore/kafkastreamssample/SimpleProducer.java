package org.kishore.kafkastreamssample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SimpleProducer extends Thread {

    public static void main(String[] args) throws JsonProcessingException {

//        insertTableEntries();
        insertStreamEntries();
    }

        static ObjectMapper mapper = new ObjectMapper();
    private static void insertStreamEntries() throws JsonProcessingException {
        String topicName = "merge_test";

        Position pos1 = new Position();
        pos1.setPosition("Lead");
        pos1.setDept("Health");
        Position pos2 = new Position();
        pos2.setPosition("Dev");
        pos2.setDept("Finance");
        Position pos3 = new Position();
        pos3.setPosition("Dev");
        pos3.setDept("Health");
        Position pos4 = new Position();
        pos4.setPosition("Lead");
        pos4.setDept("Finance");
        String s = mapper.writeValueAsString(pos1);
        String s2 = mapper.writeValueAsString(pos2);
        String s3 = mapper.writeValueAsString(pos3);
        String s4 = mapper.writeValueAsString(pos4);

        List<String> positionsList = Arrays.asList(s,s2,s3,s4);

        Producer<String, String> producer = createProducerProps();

        int i=0;
        for (String msg : positionsList) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, String.valueOf(++i), msg);
            producer.send(record);

        }
        producer.close();
        System.out.println("SimpleProducer Completed.");
    }

    private static void insertTableEntries() throws JsonProcessingException {
        String topicName = "lookup_topic";
        String key = "Lead";
        Salary value = new Salary();
        value.setSalary("95000");

        String key1 = "Dev";
        Salary value1 = new Salary();
        value.setSalary("82000");

        String valueStr = mapper.writeValueAsString(value);
        String valueStr2 = mapper.writeValueAsString(value1);

        Producer<String, String> producer = createProducerProps();

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, valueStr);
        producer.send(record);


        ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, key1, valueStr2);
        producer.send(record2);

        producer.close();

        System.out.println("SimpleProducer Completed.");
    }

    private static Producer<String, String> createProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }
}