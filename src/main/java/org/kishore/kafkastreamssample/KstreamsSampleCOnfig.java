package org.kishore.kafkastreamssample;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KstreamsSampleCOnfig {
    @Autowired
    private ObjectMapper mapper;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfig(@Value("${kafka.app.id}") String appName) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "locahost:9092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return new StreamsConfig(props);
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream("merge_test");
        GlobalKTable<String, String> globalKTable = streamsBuilder.globalTable("lookup_topic", Materialized.as("lookup_store"));
        KTable<Object, Object> table = streamsBuilder.table("");

        KStream<String, String>[] branches = stream.branch(this::testLead, this::testOther);
        KStream<String, String> leadStream = branches[0];
        KStream<String, String> otherStream = branches[1];
        leadStream.mapValues(v -> {
            try {
                Thread.sleep(1500);
//                System.out.println("Slept for sometime..");
                return v;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return v;
        });

        KStream<String, String> enrichedStream = leadStream.leftJoin(globalKTable, this::getKey, this::joinValue);
        KStream<String, String> mergedStream = otherStream.merge(enrichedStream);
        mergedStream.foreach((k, v) -> {
            log.info("MESSAGE$$$ " + v);
            try {
                Thread.sleep(750);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        return mergedStream;


    }

    private boolean testOther(String k, String v) {
        return !testLead(k, v);
    }

    private boolean testLead(String k, String v) {
        try {
            Position p = mapper.readValue(v, Position.class);
            return "Lead".equals(p.getPosition());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private String getKey(String k, String v) {
        String key = "";
        try {
            Position position = mapper.readValue(v, Position.class);
            key = position.getPosition();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return key;
    }

    private String joinValue(String rec, String lookupRec) {
        String result =null;
        try {
            Position position = mapper.readValue(rec, Position.class);
            Salary salary = mapper.readValue(lookupRec, Salary.class);
            position.setSalary(salary.getSalary());
            result = mapper.writeValueAsString(position);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
