package com.example.kafka_test;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class KafkaTestApplication {

	public static void main(String[] args) {

		SpringApplication.run(KafkaTestApplication.class, args);

	}

	@Bean
	CommandLineRunner commandLineRunner(KafkaTemplate<String,String>kafkaTemplate){
		return args ->{
			//le nom du topic est celui creer dans le fichier taftatopicConfig
		for(int i=0;i<100;i++){
			kafkaTemplate.send("test","testt kafka consumer"+i);

		}
		};
	}

}
