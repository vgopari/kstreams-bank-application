package io.streams.kstreamsbankapplication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KstreamsBankApplication {

	public static void main(String[] args) {
		SpringApplication.run(KstreamsBankApplication.class, args);
	}

}
