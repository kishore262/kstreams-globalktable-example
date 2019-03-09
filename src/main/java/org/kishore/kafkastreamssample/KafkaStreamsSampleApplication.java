package org.kishore.kafkastreamssample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamsSampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsSampleApplication.class, args);
	}

}
